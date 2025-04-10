import os
import logging
import psycopg2
from web3 import Web3
from decimal import Decimal
from datetime import datetime
from eth_account import Account

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# PostgreSQL
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")

# Web3 / Blockchain
WEB3_RPC = os.getenv("WEB3_RPC")
XP_TOKEN_CONTRACT_ADDRESS = os.getenv("XP_TOKEN_CONTRACT_ADDRESS")
XP_OWNER_PRIVATE_KEY = os.getenv("XP_OWNER_PRIVATE_KEY")
CHAIN_ID = int(os.getenv("CHAIN_ID"))

XP_TOKEN_ABI = [
    {
        "inputs": [
            {"internalType": "address", "name": "to", "type": "address"},
            {"internalType": "uint256", "name": "amount", "type": "uint256"},
        ],
        "name": "mint",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]

logging.basicConfig(level=logging.INFO)

def get_users():
    conn = psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )
    cursor = conn.cursor()
    cursor.execute("""
        SELECT u.username, up.id, up.xp_points, w.wallet_address, w.id AS wallet_id
        FROM user_userprofile up
        JOIN user_user u ON up.user_id = u.id
        JOIN user_wallet w ON w.user_id = up.id
        WHERE up.xp_points > 0
    """)
    users = cursor.fetchall()
    cursor.close()
    conn.close()
    return users

def mint_xp(wallet_address, amount, nonce, web3, contract, owner):
    tx = contract.functions.mint(
        wallet_address,
        web3.to_wei(amount, 'ether')
    ).build_transaction({
        "chainId": CHAIN_ID,
        "gas": 200000,
        "gasPrice": web3.eth.gas_price,
        "nonce": nonce,
    })

    signed_tx = owner.sign_transaction(tx) 
    tx_hash = web3.eth.send_raw_transaction(signed_tx.raw_transaction)
    receipt = web3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)

    if receipt.status == 1:
        return tx_hash.hex()
    else:
        return None


def record_transaction(wallet_id, tx_hash, user_profile_id, amount, token, chain_id, status="success", retry_count=0):
    conn = psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO user_transaction 
            (wallet_id, tx_hash, user_id, amount, token, chain_id, status, retry_count, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        wallet_id,
        tx_hash,
        user_profile_id,
        Decimal(amount),
        token,
        chain_id,
        status,
        retry_count,
        datetime.now(),
        datetime.now(),
    ))

    conn.commit()
    cursor.close()
    conn.close()
   
def has_pending_transaction(profile_id, current_xp):
    conn = psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COALESCE(SUM(amount), 0)
        FROM user_transaction
        WHERE user_id = %s AND token = 'XP'
    """, (profile_id,))
    total_minted = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    return Decimal(total_minted) >= Decimal(current_xp)    
    
def run():
    logging.info("🚀 Minting XP tokens...")
    users = get_users()
    if not users:
        logging.info("❌ No users with XP found.")
        return

    web3 = Web3(Web3.HTTPProvider(WEB3_RPC))
    owner = web3.eth.account.from_key(XP_OWNER_PRIVATE_KEY)
    nonce = web3.eth.get_transaction_count(owner.address)
    contract = web3.eth.contract(address=web3.to_checksum_address(XP_TOKEN_CONTRACT_ADDRESS), abi=XP_TOKEN_ABI)

    for username, profile_id, xp, wallet_address, wallet_id in users:
        try:
            if has_pending_transaction(profile_id, xp):
                logging.info(f"⏭️ Skipping {username}: XP already minted or equal")
                continue
        
            xp_to_mint = Decimal(xp - 1)
            logging.info(f"🔄 Minting {xp_to_mint} XP for {username} → {wallet_address}")
            tx_hash = mint_xp(wallet_address, Decimal(xp_to_mint), nonce, web3, contract)
            if tx_hash:
                record_transaction(
                    wallet_id=wallet_id,
                    tx_hash=tx_hash,
                    user_profile_id=profile_id,
                    amount=xp_to_mint,
                    token="XP",
                    chain_id=CHAIN_ID,
                    status="success",
                    retry_count=0
                )
                logging.info(f"✅ Minted {xp_to_mint} XP → TX: {tx_hash}")
            else:
                logging.error(f"❌ TX failed for {username}")
        except Exception as e:
            logging.error(f"🔥 Error minting for {username}: {e}")
        nonce += 1


def mint_to_specific_wallet():
    target_address = "0x377B8a3152abEfb9a9da776C606024Bb8b93be0F"

    conn = psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )
    cursor = conn.cursor()

    cursor.execute("""
        SELECT u.username, up.id, up.xp_points, w.wallet_address, w.id
        FROM user_userprofile up
        JOIN user_user u ON up.user_id = u.id
        JOIN user_wallet w ON w.user_id = up.id
        WHERE w.wallet_address = %s
    """, (target_address,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()

    if not result:
        logging.error("❌ Wallet not found")
        return

    username, profile_id, xp, wallet_address, wallet_id = result

    if not xp or xp <= 1:
        logging.info("❌ No users with XP found.")
        return
    logging.info(XP_OWNER_PRIVATE_KEY)
    web3 = Web3(Web3.HTTPProvider(WEB3_RPC))
    owner = Account.from_key(XP_OWNER_PRIVATE_KEY)
    nonce = web3.eth.get_transaction_count(owner.address)
    contract = web3.eth.contract(address=web3.to_checksum_address(XP_TOKEN_CONTRACT_ADDRESS), abi=XP_TOKEN_ABI)

    xp_to_mint = Decimal(xp - 1)
    try:
        logging.info(f"🔄 Minting {xp_to_mint} XP give {username} → {wallet_address}")
        tx_hash = mint_xp(wallet_address, xp_to_mint, nonce, web3, contract, owner)
        if tx_hash:
            record_transaction(
                wallet_id=wallet_id,
                tx_hash=tx_hash,
                user_profile_id=profile_id,
                amount=xp_to_mint,
                token="XP",
                chain_id=CHAIN_ID,
                status="success",
                retry_count=0
            )
            logging.info(f"✅ Success Mint {xp_to_mint} XP → TX: {tx_hash}")
        else:
            logging.error(f"❌ Mint Fail for {username}")
    except Exception as e:
        logging.error(f"🔥 ERROR: {e}")


if __name__ == "__main__":
    mint_to_specific_wallet()