#!/usr/bin/env python3
"""
Simple Couchbase Connection Setup and Test
This script helps you configure and test your Couchbase connection step by step.
"""

import os
import json
from datetime import timedelta

def check_environment_variables():
    """Check if environment variables are set"""
    print("🔍 Checking Environment Variables...")
    
    required_vars = {
        "COUCHBASE_CONNECTION_STRING":"couchbases://cb.iquykkovhzrt-1of.cloud.couchbase.com",
        'COUCHBASE_USERNAME': 'Aneeshjayan' ,
        'COUCHBASE_PASSWORD' : 'aneesh123#J',
        'COUCHBASE_BUCKET':'database_customer',
        "GOOGLE_API_KEY":"AIzaSyA5FtbbhjQB8sGgT6kvWmtLm-deyYhmrLQ"
    }
    
    missing = []
    set_vars = []
    
    for var, description in required_vars.items():
        value = os.getenv(var)
        if value:
            # Mask sensitive values
            if 'PASSWORD' in var or 'KEY' in var:
                display_value = f"{value[:8]}..." if len(value) > 8 else "***"
            else:
                display_value = value
            set_vars.append(f"  ✅ {var} = {display_value}")
        else:
            missing.append(f"  ❌ {var} - {description}")
    
    if set_vars:
        print("Set variables:")
        for var in set_vars:
            print(var)
    
    if missing:
        print("\nMissing variables:")
        for var in missing:
            print(var)
        return False
    
    return True

def create_env_template():
    """Create a .env file template"""
    template = """# Couchbase Cloud Configuration
# Get these from your Couchbase Cloud console
COUCHBASE_CONNECTION_STRING=couchbases://cb.your-cluster.cloud.couchbase.com
COUCHBASE_USERNAME=your-username
COUCHBASE_PASSWORD=your-password
COUCHBASE_BUCKET=default

# Google Gemini API Key
# Get this from https://aistudio.google.com/app/apikey
GOOGLE_API_KEY=your-google-api-key
"""
    
    if not os.path.exists('.env'):
        with open('.env', 'w') as f:
            f.write(template)
        print("✅ Created .env template file")
        print("📝 Please edit .env file with your actual credentials")
    else:
        print("📄 .env file already exists")

def manual_input_setup():
    """Get credentials manually from user input"""
    print("\n🔧 Manual Setup - Enter your credentials:")
    print("(Press Enter to skip and use environment variables)")
    
    connection_string = input("\nCouchbase Connection String (couchbases://...): ").strip()
    username = input("Couchbase Username: ").strip()
    password = input("Couchbase Password: ").strip()
    bucket = input("Bucket Name (default: 'default'): ").strip() or "default"
    
    if connection_string and username and password:
        return {
            'connection_string': connection_string,
            'username': username,
            'password': password,
            'bucket': bucket
        }
    return None

def test_couchbase_connection_simple(credentials=None):
    """Simple Couchbase connection test"""
    print("\n🔗 Testing Couchbase Connection...")
    
    try:
        from couchbase.cluster import Cluster
        from couchbase.auth import PasswordAuthenticator
        from couchbase.options import ClusterOptions
    except ImportError:
        print("❌ Couchbase SDK not installed. Install with:")
        print("   pip install couchbase")
        return False
    
    # Use provided credentials or environment variables
    if credentials:
        connection_string = credentials['connection_string']
        username = credentials['username']
        password = credentials['password']
        bucket_name = credentials['bucket']
    else:
        connection_string = os.getenv('COUCHBASE_CONNECTION_STRING')
        username = os.getenv('COUCHBASE_USERNAME')
        password = os.getenv('COUCHBASE_PASSWORD')
        bucket_name = os.getenv('COUCHBASE_BUCKET', 'default')
    
    if not all([connection_string, username, password]):
        print("❌ Missing connection credentials")
        return False
    
    print(f"🔗 Connection: {connection_string}")
    print(f"👤 Username: {username}")
    print(f"🪣 Bucket: {bucket_name}")
    
    try:
        # Create authentication
        auth = PasswordAuthenticator(username, password)
        
        # Create cluster options
        options = ClusterOptions(auth)
        
        # Add profile for cloud connections
        try:
            options.apply_profile("wan_development")
            print("✅ Applied WAN development profile")
        except:
            print("⚠️ Could not apply WAN profile, continuing...")
        
        # Connect to cluster
        print("🔄 Connecting to cluster...")
        cluster = Cluster(connection_string, options)
        
        # Wait for ready with extended timeout
        print("⏳ Waiting for cluster to be ready (30 seconds timeout)...")
        cluster.wait_until_ready(timedelta(seconds=30))
        print("✅ Cluster is ready!")
        
        # Get bucket
        print(f"🪣 Accessing bucket '{bucket_name}'...")
        bucket = cluster.bucket(bucket_name)
        
        # Get default collection
        print("📄 Accessing default collection...")
        collection = bucket.scope("_default").collection("_default")
        
        # Test with a simple document operation
        print("🧪 Testing document operations...")
        test_doc_id = "test_connection_doc"
        test_data = {
            "test": True,
            "message": "Connection test successful",
            "timestamp": "2024-01-01T00:00:00Z"
        }
        
        # Try to insert/upsert a test document
        collection.upsert(test_doc_id, test_data)
        print("✅ Document write successful!")
        
        # Try to read the document back
        result = collection.get(test_doc_id)
        retrieved_data = result.content_as[dict]
        print("✅ Document read successful!")
        
        # Clean up test document
        collection.remove(test_doc_id)
        print("✅ Test cleanup successful!")
        
        print("\n🎉 Couchbase connection test PASSED!")
        print("✅ All operations work correctly!")
        return True
        
    except Exception as e:
        print(f"\n❌ Connection test FAILED: {e}")
        print("\n🔍 Troubleshooting tips:")
        print("1. Check your connection string format: couchbases://cb.xxx.cloud.couchbase.com")
        print("2. Verify username and password are correct")
        print("3. Ensure your IP address is whitelisted in Couchbase Cloud")
        print("4. Confirm the bucket exists and you have access")
        print("5. Check your internet connection")
        print("6. Try creating a new bucket called 'default' in Couchbase Cloud")
        return False

def test_google_api():
    """Test Google Gemini API"""
    print("\n🤖 Testing Google Gemini API...")
    
    api_key = os.getenv('GOOGLE_API_KEY')
    if not api_key:
        print("❌ GOOGLE_API_KEY environment variable not set")
        return False
    
    try:
        from langchain_google_genai import ChatGoogleGenerativeAI
    except ImportError:
        print("❌ Google Gemini SDK not installed. Install with:")
        print("   pip install langchain-google-genai")
        return False
    
    try:
        llm = ChatGoogleGenerativeAI(
            model="gemini-1.5-flash",
            google_api_key=api_key,
            temperature=0
        )
        
        response = llm.invoke("Reply with exactly: 'API test successful'")
        print(f"✅ Gemini API response: {response.content}")
        return True
        
    except Exception as e:
        print(f"❌ Gemini API test failed: {e}")
        print("\n🔍 Get your API key from: https://aistudio.google.com/app/apikey")
        return False

def main():
    """Main setup and test function"""
    print("🚀 Couchbase + Gemini Setup and Test")
    print("=" * 50)
    
    # Load .env file if it exists
    try:
        from dotenv import load_dotenv
        load_dotenv()
        print("✅ Loaded .env file")
    except ImportError:
        print("⚠️ python-dotenv not installed, using system environment variables")
        print("   Install with: pip install python-dotenv")
    except:
        pass
    
    # Step 1: Check environment variables
    print("\n" + "="*50)
    env_ok = check_environment_variables()
    
    # Step 2: Offer to create template or manual input
    if not env_ok:
        print("\n📋 Setup Options:")
        print("1. Create .env template file")
        print("2. Enter credentials manually for testing")
        print("3. Exit and set environment variables manually")
        
        choice = input("\nChoose option (1/2/3): ").strip()
        
        if choice == "1":
            create_env_template()
            print("\n📝 Please edit .env file and run this script again")
            return
        elif choice == "2":
            credentials = manual_input_setup()
            if credentials:
                success = test_couchbase_connection_simple(credentials)
                if success:
                    print("\n✅ Manual test successful!")
                    print("💡 Consider setting these as environment variables")
                return
            else:
                print("❌ Insufficient credentials provided")
                return
        else:
            return
    
    # Step 3: Test connections
    print("\n" + "="*50)
    print("🧪 Running Connection Tests...")
    
    # Test Couchbase
    couchbase_ok = test_couchbase_connection_simple()
    
    # Test Google API
    google_ok = test_google_api()
    
    # Final summary
    print("\n" + "="*50)
    print("📊 Test Results Summary:")
    print(f"  Environment Variables: {'✅' if env_ok else '❌'}")
    print(f"  Couchbase Connection:  {'✅' if couchbase_ok else '❌'}")
    print(f"  Google Gemini API:     {'✅' if google_ok else '❌'}")
    
    if env_ok and couchbase_ok and google_ok:
        print("\n🎉 ALL TESTS PASSED! 🎉")
        print("✅ You're ready to run the full CRUD agent!")
        print("🚀 Run your main script now!")
    else:
        print("\n⚠️ Some tests failed. Please fix the issues above.")
        print("\n💡 Common solutions:")
        if not couchbase_ok:
            print("   - Verify your Couchbase Cloud cluster is running")
            print("   - Check IP whitelist in Couchbase Cloud security settings")
            print("   - Confirm bucket exists and user has permissions")
        if not google_ok:
            print("   - Get API key from https://aistudio.google.com/app/apikey")
            print("   - Ensure you have API quota/billing enabled")

if __name__ == "__main__":
    main()