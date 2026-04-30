import hashlib
import json
import time
import os
from flask import Flask, jsonify, request
from pymongo import MongoClient, DESCENDING, ASCENDING
from pymongo.errors import DuplicateKeyError

app = Flask(__name__)

# -------------------------------------------------------------------
# MongoDB সংযোগ (পরিবেশ ভ্যারিয়েবল থেকে)
# -------------------------------------------------------------------
MONGO_URI = os.environ.get("MONGO_URI")
if not MONGO_URI:
    raise RuntimeError("MONGO_URI environment variable not set!")

client = MongoClient(MONGO_URI)
db = client["simplecoin"]

# কালেকশন
blocks_col = db["blocks"]
tx_pool_col = db["tx_pool"]
accounts_col = db["accounts"]  # { "_id": "address", "balance": float }

# ইনডেক্স তৈরি (প্রথম বারের জন্য)
blocks_col.create_index([("index", DESCENDING)], unique=True)
accounts_col.create_index("_id", unique=True)
tx_pool_col.create_index("timestamp")

# -------------------------------------------------------------------
# Blockchain ক্লাস (MongoDB-ভিত্তিক)
# -------------------------------------------------------------------
class Blockchain:
    def __init__(self, difficulty=3):
        self.difficulty = difficulty
        self.mining_reward = 1.0
        self.transaction_fee = 0.001  # প্রতিটি ট্রানজেকশনের ফি
        # জেনেসিস ব্লক তৈরি (যদি না থাকে)
        if blocks_col.count_documents({}) == 0:
            self._create_genesis_block()

    def _create_genesis_block(self):
        """প্রথম ব্লক তৈরি করে ডাটাবেসে সংরক্ষণ"""
        genesis = {
            "index": 0,
            "timestamp": time.time(),
            "transactions": [],
            "proof": 100,
            "previous_hash": "0"
        }
        genesis["hash"] = self.hash(genesis)
        blocks_col.insert_one(genesis)
        # জেনেসিস ব্লকের জন্য বিশেষ কোনো ব্যালেন্স পরিবর্তন নেই

    @staticmethod
    def hash(block):
        # ব্লক থেকে হ্যাশ করতে আমরা সম্পূর্ণ ডাম্প করব, কিন্তু sort_keys=True এবং নির্দিষ্ট ফিল্ড নেব
        block_str = json.dumps(block, sort_keys=True).encode()
        return hashlib.sha256(block_str).hexdigest()

    @property
    def last_block(self):
        return blocks_col.find_one(sort=[("index", DESCENDING)])

    def new_transaction(self, sender, recipient, amount):
        # ব্যালেন্স চেক
        if sender != "0":  # কয়েনবেজ নয়
            sender_acc = accounts_col.find_one({"_id": sender})
            if not sender_acc or sender_acc["balance"] < amount + self.transaction_fee:
                raise ValueError("অপর্যাপ্ত ব্যালেন্স বা ফি দেওয়ার মতো কয়েন নেই")

        # পেন্ডিং ট্রানজেকশন পুলে জমা
        tx = {
            "sender": sender,
            "recipient": recipient,
            "amount": amount,
            "fee": self.transaction_fee if sender != "0" else 0.0,
            "timestamp": time.time()
        }
        tx_pool_col.insert_one(tx)
        return self.last_block["index"] + 1  # যে ব্লকে যাবে

    def proof_of_work(self, last_proof):
        proof = 0
        while not self.valid_proof(last_proof, proof):
            proof += 1
        return proof

    def valid_proof(self, last_proof, proof):
        guess = f"{last_proof}{proof}".encode()
        guess_hash = hashlib.sha256(guess).hexdigest()
        return guess_hash[:self.difficulty] == "0" * self.difficulty

    def mine(self, miner_address):
        """মাইনিং: পেন্ডিং ট্রানজেকশন নেয়, পুরস্কার ও ফি অ্যাডজাস্ট করে ব্লক তৈরি"""
        last_block = self.last_block
        last_proof = last_block["proof"]
        proof = self.proof_of_work(last_proof)

        # পেন্ডিং ট্রানজেকশন সংগ্রহ (সর্বোচ্চ 100 টা)
        pending_txs = list(tx_pool_col.find().sort("timestamp", ASCENDING).limit(100))
        if not pending_txs:
            # কোনো ট্রানজেকশন না থাকলেও ব্লক তৈরি করব (শুধু কয়েনবেজ পুরস্কার)
            pass

        # কয়েনবেজ (মাইনার পুরস্কার)
        coinbase_tx = {
            "sender": "0",
            "recipient": miner_address,
            "amount": self.mining_reward,
            "fee": 0.0
        }
        # ট্রানজেকশন ফি ক্যালকুলেট ও মাইনারকে দেওয়া
        total_fee = 0
        processed_txs = []
        for tx in pending_txs:
            if tx["sender"] != "0":
                total_fee += tx.get("fee", 0)
            processed_txs.append({
                "sender": tx["sender"],
                "recipient": tx["recipient"],
                "amount": tx["amount"],
                "fee": tx.get("fee", 0)
            })
        # ফি পুরস্কার
        coinbase_tx["amount"] += total_fee

        # সব ট্রানজেকশন একত্রে
        block_transactions = [coinbase_tx] + processed_txs

        # ব্যালেন্স আপডেট শুরু (এটোমিক না, তবে সাধারণ ব্যবহারের জন্য এটুকুই)
        for tx in block_transactions:
            sender = tx["sender"]
            recipient = tx["recipient"]
            amount = tx["amount"]
            fee = tx.get("fee", 0)
            # সেন্ডার ব্যালেন্স কমানো (coinbase ছাড়া)
            if sender != "0":
                accounts_col.update_one(
                    {"_id": sender},
                    {"$inc": {"balance": -(amount + fee)}},
                    upsert=False
                )
            # রিসিপিয়েন্ট ব্যালেন্স বাড়ানো
            accounts_col.update_one(
                {"_id": recipient},
                {"$inc": {"balance": amount}},
                upsert=True  # না থাকলে নতুন অ্যাকাউন্ট 0 থেকে শুরু
            )

        # ব্লক তৈরি ও সেভ
        new_block = {
            "index": last_block["index"] + 1,
            "timestamp": time.time(),
            "transactions": block_transactions,
            "proof": proof,
            "previous_hash": last_block.get("hash", "")
        }
        new_block["hash"] = self.hash(new_block)
        blocks_col.insert_one(new_block)

        # পেন্ডিং ট্রানজেকশন মুছে ফেলা
        if pending_txs:
            ids = [tx["_id"] for tx in pending_txs]
            tx_pool_col.delete_many({"_id": {"$in": ids}})

        return new_block

    def get_balance(self, address):
        acc = accounts_col.find_one({"_id": address})
        return acc["balance"] if acc else 0.0

# ব্লকচেইন ইনস্ট্যান্স
blockchain = Blockchain(difficulty=3)

# রেন্ডার নোড অ্যাড্রেস (হোস্টনেম)
node_address = os.environ.get("RENDER_EXTERNAL_HOSTNAME", "localhost")

# -------------------------------------------------------------------
# API রাউট
# -------------------------------------------------------------------
@app.route("/", methods=["GET"])
def home():
    return jsonify({"message": "Advanced SimpleCoin running on Render + MongoDB"})

@app.route("/chain", methods=["GET"])
def get_chain():
    chain = list(blocks_col.find({}, {"_id": 0}).sort("index", ASCENDING))
    return jsonify({"chain": chain, "length": len(chain)})

@app.route("/mine", methods=["GET"])
def mine():
    try:
        block = blockchain.mine(node_address)
        return jsonify({
            "message": "নতুন ব্লক মাইন করা হয়েছে",
            "block": block
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/transactions/new", methods=["POST"])
def new_transaction():
    values = request.get_json()
    required = ["sender", "recipient", "amount"]
    if not all(k in values for k in required):
        return jsonify({"error": "অসম্পূর্ণ ডাটা"}), 400

    try:
        future_block = blockchain.new_transaction(
            values["sender"],
            values["recipient"],
            float(values["amount"])
        )
        return jsonify({
            "message": f"ট্রানজেকশন পেন্ডিং পুলে যোগ হয়েছে, ব্লক {future_block} এ অন্তর্ভুক্ত হবে"
        }), 201
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

@app.route("/balance/<address>", methods=["GET"])
def get_balance(address):
    bal = blockchain.get_balance(address)
    return jsonify({"address": address, "balance": bal})

@app.route("/mempool", methods=["GET"])
def get_mempool():
    pool = list(tx_pool_col.find({}, {"_id": 0}).sort("timestamp", ASCENDING))
    return jsonify({"pending_transactions": pool, "count": len(pool)})

@app.route("/nodes/register", methods=["POST"])
def register_node():
    # এই ডেমোতে নোড সঞ্চয় করা হয় না, শুধু দেখানোর জন্য
    values = request.get_json()
    return jsonify({"message": "নোড রেজিস্ট্রেশন ডেমো", "nodes": values.get("nodes", [])}), 201

# -------------------------------------------------------------------
# মেইন
# -------------------------------------------------------------------
if __name__ == "__main__":
    # Gunicorn ব্যবহার করলে সরাসরি নিচের লাইন দরকার নেই, তবে রেন্ডারের build এটাই ব্যবহার হবে
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False)