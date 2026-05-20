#!/usr/bin/env python3
"""
FreeAPI Ingestion Dry-Run Verification Script (With Robust Self-Healing Login)
Tests the self-healing DB seeding, admin login, and product/order data fetching logic.
"""

import sys
import json
import time
import requests

BASE_URL = "https://api.freeapi.app/api/v1"

def dry_run():
    print("=== STARTING FREEAPI INGESTION DRY-RUN ===")
    print(f"Connecting to: {BASE_URL}")
    
    cred_url = f"{BASE_URL}/seed/generated-credentials"
    users = []
    
    # 1. Try to get credentials
    print("\n[Step 1] Checking if database credentials file exists...")
    try:
        response = requests.get(cred_url, timeout=20)
        if response.status_code == 200:
            users = response.json().get("data", [])
            print(f"✅ Credentials file found with {len(users)} users.")
        else:
            print("Credentials file not found (404). Database needs seeding.")
    except Exception as e:
        print(f"Failed to check credentials file: {e}")
        
    # If no users, or we need to seed
    if not users:
        print("Triggering database seeding (ecommerce)...")
        seed_res = requests.post(f"{BASE_URL}/seed/ecommerce", timeout=60)
        if seed_res.status_code in [200, 201]:
            print("Seeding successful! Waiting 3 seconds for indices...")
            time.sleep(3)
            response = requests.get(cred_url, timeout=20)
            response.raise_for_status()
            users = response.json().get("data", [])
        else:
            print(f"❌ Seeding failed with status {seed_res.status_code}: {seed_res.text}")
            return False

    # 2. Extract Admin Login details & Attempt Login
    print("\n[Step 2] Authenticating as Admin...")
    admin_user = next((u for u in users if u.get("role") == "ADMIN"), None)
    if not admin_user:
        print("❌ Error: No ADMIN user found in credentials list.")
        return False
        
    login_url = f"{BASE_URL}/users/login"
    login_payload = {
        "username": admin_user["username"],
        "password": admin_user["password"]
    }
    
    login_res = requests.post(login_url, json=login_payload, timeout=20)
    
    # If login returns 404 (stale credentials after database reset), trigger re-seeding!
    if login_res.status_code == 404:
        print("⚠️ Stale credentials found in cache (User does not exist). Re-seeding database...")
        seed_res = requests.post(f"{BASE_URL}/seed/ecommerce", timeout=60)
        if seed_res.status_code in [200, 201]:
            print("Re-seeding successful! Waiting 3 seconds...")
            time.sleep(3)
            # Fetch fresh credentials
            response = requests.get(cred_url, timeout=20)
            response.raise_for_status()
            users = response.json().get("data", [])
            admin_user = next((u for u in users if u.get("role") == "ADMIN"), None)
            if not admin_user:
                print("❌ Error: No ADMIN user found in fresh credentials list.")
                return False
            login_payload = {
                "username": admin_user["username"],
                "password": admin_user["password"]
            }
            # Try login again
            print("Logging in with fresh credentials...")
            login_res = requests.post(login_url, json=login_payload, timeout=20)
        else:
            print(f"❌ Re-seeding failed: {seed_res.text}")
            return False
            
    login_res.raise_for_status()
    token = login_res.json().get("data", {}).get("accessToken")
    if not token:
        print("❌ Error: Access token was not returned in the login payload.")
        return False
    print(f"✅ Authenticated successfully as admin user '{admin_user['username']}'.")

    # 3. Fetch Products (Public API)
    print("\n[Step 3] Fetching product catalog (Public API)...")
    prod_url = f"{BASE_URL}/ecommerce/products?page=1&limit=5"
    prod_res = requests.get(prod_url, timeout=20)
    prod_res.raise_for_status()
    products = prod_res.json().get("data", {}).get("products", [])
    print(f"✅ Successfully fetched {len(products)} products.")
    if products:
        print(f"   Sample Product: ID={products[0].get('_id')}, Name='{products[0].get('name')}', Price=${products[0].get('price')}")
        
    # 4. Fetch Orders (Admin API)
    print("\n[Step 4] Fetching order list (Admin API)...")
    headers = {"Authorization": f"Bearer {token}"}
    order_url = f"{BASE_URL}/ecommerce/orders/list/admin?page=1&limit=5"
    order_res = requests.get(order_url, headers=headers, timeout=20)
    order_res.raise_for_status()
    orders = order_res.json().get("data", {}).get("orders", [])
    print(f"✅ Successfully fetched {len(orders)} orders.")
    if orders:
        print(f"   Sample Order: ID={orders[0].get('_id')}, Status='{orders[0].get('orderStatus')}', Price=${orders[0].get('orderPrice')}")
        
    print("\n=== DRY-RUN COMPLETED SUCCESSFULLY! ===")
    print("All API connections and self-healing extraction models are working perfectly.")
    return True

if __name__ == "__main__":
    success = dry_run()
    sys.exit(0 if success else 1)
