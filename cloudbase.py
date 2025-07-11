#!/usr/bin/env python3
"""
Robust CouchBase Agent with Better Connection Handling and Diagnostics - Using Gemini API
"""

from typing import Dict, Any, Optional, List, Union
import json
import logging
import os
import asyncio
from datetime import datetime, timedelta
from collections import defaultdict
from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions, QueryOptions, ClusterTimeoutOptions
from couchbase.exceptions import CouchbaseException, DocumentNotFoundException
import couchbase.subdocument as SD
import google.generativeai as genai
from google.generativeai.types import HarmCategory, HarmBlockThreshold
import re
import httpx

# Set credentials directly in code (replace with your actual values)
os.environ["CB_CONNECTION_STRING"] = "couchbases://cb.iquykkovhzrt-1of.cloud.couchbase.com"
os.environ["CB_USERNAME"] = "Aneeshjayan"
os.environ["CB_PASSWORD"] = "aneesh123#J"
os.environ["CB_BUCKET_NAME"] = "database_customer"
os.environ["GEMINI_API_KEY"] = "AIzaSyA5FtbbhjQB8sGgT6kvWmtLm-deyYhmrLQ"  # Replace with your actual Gemini API key
os.environ["READ_ONLY_QUERY_MODE"] = "true"

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RobustCouchbaseClient:
    """Robust CouchBase client with better error handling and diagnostics"""
    
    def __init__(self):
        self.cluster = None
        self.bucket = None
        self.is_connected = False
        self.connection_healthy = False
        self.credentials = self._load_credentials()
        self.read_only_mode = self.credentials.get("read_only_mode", "true").lower() == "true"
        
        # Schema knowledge storage
        self.database_schema = {
            "scopes": {},
            "collections": {},
            "field_mappings": {},
            "indexes": {},
            "relationships": {},
            "last_analyzed": None
        }
        self.schema_learned = False
        self.connection_status = "not_connected"
        self.last_error = None
    
    def _load_credentials(self) -> Dict[str, str]:
        """Load CouchBase credentials from environment variables"""
        credentials = {
            "connection_string": os.getenv("CB_CONNECTION_STRING", ""),
            "username": os.getenv("CB_USERNAME", ""),
            "password": os.getenv("CB_PASSWORD", ""),
            "bucket_name": os.getenv("CB_BUCKET_NAME", "database_customer"),
            "read_only_mode": os.getenv("READ_ONLY_QUERY_MODE", "true")
        }
        return credentials
    
    def validate_credentials(self) -> tuple[bool, str]:
        """Validate that required credentials are present"""
        required_fields = ["connection_string", "username", "password"]
        missing_fields = []
        
        for field in required_fields:
            if not self.credentials.get(field):
                missing_fields.append(field.upper())
        
        if missing_fields:
            return False, f"Missing required environment variables: {', '.join(missing_fields)}"
        
        return True, "All credentials present"
    
    def test_connection_health(self) -> Dict[str, Any]:
        """Test various aspects of the connection"""
        health_report = {
            "overall_status": "unknown",
            "cluster_connected": False,
            "bucket_accessible": False,
            "query_working": False,
            "bucket_exists": False,
            "permissions_ok": False,
            "error_details": []
        }
        
        try:
            # Test 1: Cluster connection
            if self.cluster:
                try:
                    ping_result = self.cluster.ping()
                    health_report["cluster_connected"] = True
                except Exception as e:
                    health_report["error_details"].append(f"Cluster ping failed: {str(e)}")
            
            # Test 2: Bucket access
            if self.bucket:
                try:
                    # Try to get bucket name (this tests bucket access)
                    bucket_name = self.bucket.name
                    health_report["bucket_accessible"] = True
                    health_report["bucket_exists"] = True
                except Exception as e:
                    health_report["error_details"].append(f"Bucket access failed: {str(e)}")
            
            # Test 3: Simple query
            try:
                simple_query = "SELECT 1 as test"
                result = list(self.cluster.query(simple_query))
                if result and result[0].get("test") == 1:
                    health_report["query_working"] = True
                    health_report["permissions_ok"] = True
            except Exception as e:
                health_report["error_details"].append(f"Query test failed: {str(e)}")
            
            # Test 4: Bucket-specific query
            try:
                bucket_name = self.credentials['bucket_name']
                bucket_query = f"SELECT 1 as test FROM `{bucket_name}` LIMIT 1"
                result = list(self.cluster.query(bucket_query))
                health_report["bucket_exists"] = True
            except Exception as e:
                health_report["error_details"].append(f"Bucket query failed: {str(e)}")
                # Bucket might not exist, let's check what buckets are available
                try:
                    available_buckets = self.get_available_buckets()
                    health_report["available_buckets"] = available_buckets
                except Exception as e2:
                    health_report["error_details"].append(f"Cannot list buckets: {str(e2)}")
            
            # Determine overall status
            if health_report["cluster_connected"] and health_report["query_working"]:
                if health_report["bucket_accessible"]:
                    health_report["overall_status"] = "healthy"
                else:
                    health_report["overall_status"] = "cluster_ok_bucket_issues"
            else:
                health_report["overall_status"] = "connection_issues"
                
        except Exception as e:
            health_report["error_details"].append(f"Health check failed: {str(e)}")
            health_report["overall_status"] = "critical_error"
        
        return health_report
    
    def get_available_buckets(self) -> List[str]:
        """Get list of available buckets"""
        try:
            # Try to get bucket list from system keyspace
            buckets_query = "SELECT name FROM system:buckets"
            result = list(self.cluster.query(buckets_query))
            return [row["name"] for row in result]
        except Exception as e:
            logger.error(f"Cannot get bucket list: {e}")
            return []
    
    async def connect_with_robust_retry(self, max_retries: int = 3) -> bool:
        """Connect with multiple strategies and detailed error reporting"""
        connection_strategies = [
            ("standard", self._connect_standard),
            ("extended_timeout", self._connect_extended_timeout),
            ("minimal_config", self._connect_minimal)
        ]
        
        for attempt in range(max_retries):
            print(f"\n🔄 Connection attempt {attempt + 1}/{max_retries}")
            
            for strategy_name, strategy_func in connection_strategies:
                try:
                    print(f"   📡 Trying {strategy_name} connection...")
                    if await strategy_func():
                        self.is_connected = True
                        self.connection_status = "connected"
                        
                        # Test connection health
                        print("   🏥 Testing connection health...")
                        health = self.test_connection_health()
                        self.connection_healthy = health["overall_status"] == "healthy"
                        
                        if self.connection_healthy:
                            print("   ✅ Connection healthy and ready!")
                            return True
                        else:
                            print(f"   ⚠️ Connection issues detected: {health['overall_status']}")
                            print(f"   📋 Health report: {json.dumps(health, indent=2)}")
                            
                            # Continue to try other strategies or provide fallback
                            if health["query_working"]:
                                print("   ➡️ Basic queries work, continuing with limited functionality...")
                                return True
                    
                except Exception as e:
                    print(f"   ❌ {strategy_name} failed: {str(e)[:100]}...")
                    self.last_error = str(e)
                    continue
            
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 3
                print(f"   ⏰ Waiting {wait_time} seconds before next attempt...")
                await asyncio.sleep(wait_time)
        
        self.connection_status = "failed"
        return False
    
    async def _connect_standard(self) -> bool:
        """Standard connection approach"""
        auth = PasswordAuthenticator(
            self.credentials["username"], 
            self.credentials["password"]
        )
        
        timeout_options = ClusterTimeoutOptions(
            connect_timeout=timedelta(seconds=20),
            kv_timeout=timedelta(seconds=5),
            query_timeout=timedelta(seconds=20)
        )
        
        cluster_options = ClusterOptions(
            authenticator=auth,
            timeout_options=timeout_options
        )
        
        self.cluster = Cluster(self.credentials["connection_string"], cluster_options)
        
        await asyncio.get_event_loop().run_in_executor(
            None, lambda: self.cluster.wait_until_ready(timedelta(seconds=15))
        )
        
        self.bucket = self.cluster.bucket(self.credentials["bucket_name"])
        
        # Test with simple query
        await asyncio.get_event_loop().run_in_executor(
            None, lambda: list(self.cluster.query("SELECT 1"))
        )
        
        return True
    
    async def _connect_extended_timeout(self) -> bool:
        """Extended timeout connection"""
        auth = PasswordAuthenticator(
            self.credentials["username"], 
            self.credentials["password"]
        )
        
        timeout_options = ClusterTimeoutOptions(
            connect_timeout=timedelta(seconds=45),
            kv_timeout=timedelta(seconds=15),
            query_timeout=timedelta(seconds=45)
        )
        
        cluster_options = ClusterOptions(
            authenticator=auth,
            timeout_options=timeout_options
        )
        
        self.cluster = Cluster(self.credentials["connection_string"], cluster_options)
        
        await asyncio.get_event_loop().run_in_executor(
            None, lambda: self.cluster.wait_until_ready(timedelta(seconds=30))
        )
        
        self.bucket = self.cluster.bucket(self.credentials["bucket_name"])
        
        return True
    
    async def _connect_minimal(self) -> bool:
        """Minimal connection configuration"""
        auth = PasswordAuthenticator(
            self.credentials["username"], 
            self.credentials["password"]
        )
        
        self.cluster = Cluster(self.credentials["connection_string"], ClusterOptions(authenticator=auth))
        self.bucket = self.cluster.bucket(self.credentials["bucket_name"])
        
        # Very basic test
        await asyncio.get_event_loop().run_in_executor(
            None, lambda: list(self.cluster.query("SELECT 1"))
        )
        
        return True
    
    def learn_schema_safe(self, deep_analysis: bool = True) -> Dict[str, Any]:
        """Safe schema learning with error handling"""
        if not self.is_connected:
            return {"error": "Not connected to database", "suggestion": "Try connecting first"}
        
        print("🧠 Learning database schema safely...")
        
        try:
            # Step 1: Check if bucket exists and is accessible
            bucket_name = self.credentials['bucket_name']
            available_buckets = self.get_available_buckets()
            
            if available_buckets and bucket_name not in available_buckets:
                return {
                    "error": f"Bucket '{bucket_name}' not found",
                    "available_buckets": available_buckets,
                    "suggestion": f"Try using one of these buckets: {', '.join(available_buckets)}"
                }
            
            # Step 2: Get scopes and collections safely
            print("   📊 Discovering database structure...")
            try:
                bucket_manager = self.bucket.collections()
                scopes = bucket_manager.get_all_scopes()
                
                schema_info = {
                    "bucket_name": bucket_name,
                    "total_scopes": len(scopes),
                    "scopes": {}
                }
                
                for scope in scopes:
                    scope_name = scope.name
                    collections_info = []
                    
                    for collection in scope.collections:
                        collection_name = collection.name
                        collection_key = f"{scope_name}.{collection_name}"
                        
                        print(f"      🔍 Analyzing {collection_key}...")
                        
                        # Safe collection analysis
                        try:
                            if deep_analysis:
                                collection_analysis = self._analyze_collection_safe(scope_name, collection_name)
                            else:
                                collection_analysis = self._quick_collection_check(scope_name, collection_name)
                            
                            collections_info.append({
                                "name": collection_name,
                                "analysis": collection_analysis
                            })
                            
                        except Exception as e:
                            collections_info.append({
                                "name": collection_name,
                                "analysis": {"error": str(e), "accessible": False}
                            })
                    
                    schema_info["scopes"][scope_name] = {
                        "collections": collections_info,
                        "collection_count": len(collections_info)
                    }
                
                self.database_schema = schema_info
                self.schema_learned = True
                self.database_schema["last_analyzed"] = datetime.now().isoformat()
                
                print("✅ Schema learning completed successfully!")
                return {"success": True, "schema": schema_info}
                
            except Exception as e:
                return {
                    "error": f"Cannot access bucket structure: {str(e)}",
                    "suggestion": "Check bucket permissions or try a different bucket"
                }
                
        except Exception as e:
            return {
                "error": f"Schema learning failed: {str(e)}",
                "connection_status": self.connection_status,
                "last_error": self.last_error
            }
    
    def _analyze_collection_safe(self, scope: str, collection: str, sample_size: int = 20) -> Dict[str, Any]:
        """Safely analyze a collection with error handling"""
        try:
            bucket_name = self.credentials['bucket_name']
            
            # First, check if collection has any documents
            count_query = f"SELECT COUNT(*) as total FROM `{bucket_name}`.`{scope}`.`{collection}`"
            count_result = list(self.cluster.query(count_query))[0]
            total_count = count_result["total"]
            
            if total_count == 0:
                return {
                    "document_count": 0,
                    "status": "empty_collection",
                    "fields": {}
                }
            
            # Get sample documents
            sample_query = f"SELECT * FROM `{bucket_name}`.`{scope}`.`{collection}` LIMIT {min(sample_size, total_count)}"
            result = self.cluster.query(sample_query)
            documents = [row for row in result]
            
            # Analyze field structure
            all_fields = set()
            field_info = defaultdict(lambda: {"types": set(), "sample_values": [], "frequency": 0})
            
            for doc in documents:
                for field, value in doc.items():
                    all_fields.add(field)
                    field_info[field]["frequency"] += 1
                    field_info[field]["types"].add(type(value).__name__)
                    
                    if len(field_info[field]["sample_values"]) < 3 and not isinstance(value, (dict, list)):
                        field_info[field]["sample_values"].append(value)
            
            # Format results
            fields = {}
            for field in all_fields:
                info = field_info[field]
                fields[field] = {
                    "types": list(info["types"]),
                    "frequency": info["frequency"],
                    "percentage": round((info["frequency"] / len(documents)) * 100, 2),
                    "sample_values": info["sample_values"],
                    "is_required": info["frequency"] == len(documents)
                }
            
            return {
                "document_count": total_count,
                "sample_size": len(documents),
                "field_count": len(all_fields),
                "fields": fields,
                "status": "analyzed_successfully"
            }
            
        except Exception as e:
            return {
                "error": str(e),
                "status": "analysis_failed"
            }
    
    def _quick_collection_check(self, scope: str, collection: str) -> Dict[str, Any]:
        """Quick check of collection accessibility"""
        try:
            bucket_name = self.credentials['bucket_name']
            
            # Just count documents
            count_query = f"SELECT COUNT(*) as total FROM `{bucket_name}`.`{scope}`.`{collection}`"
            count_result = list(self.cluster.query(count_query))[0]
            total_count = count_result["total"]
            
            return {
                "document_count": total_count,
                "status": "accessible",
                "quick_check": True
            }
            
        except Exception as e:
            return {
                "error": str(e),
                "status": "not_accessible"
            }
    
    def get_connection_diagnosis(self) -> str:
        """Get detailed connection diagnosis"""
        diagnosis = []
        diagnosis.append(f"🔍 Connection Diagnosis for {self.credentials['bucket_name']}")
        diagnosis.append("=" * 60)
        diagnosis.append(f"📊 Connection Status: {self.connection_status}")
        diagnosis.append(f"🏥 Health Status: {'Healthy' if self.connection_healthy else 'Issues detected'}")
        
        if self.last_error:
            diagnosis.append(f"❌ Last Error: {self.last_error}")
        
        if self.is_connected:
            health = self.test_connection_health()
            diagnosis.append("\n🏥 Health Report:")
            for key, value in health.items():
                if key != "error_details":
                    diagnosis.append(f"   {key}: {value}")
            
            if health.get("error_details"):
                diagnosis.append("\n⚠️ Issues Found:")
                for error in health["error_details"]:
                    diagnosis.append(f"   • {error}")
            
            if health.get("available_buckets"):
                diagnosis.append(f"\n📁 Available Buckets: {', '.join(health['available_buckets'])}")
        
        return "\n".join(diagnosis)
    
    def get_credentials_summary(self) -> str:
        """Get a summary of loaded credentials"""
        return f"""
🔗 Connection String: {self.credentials.get('connection_string', 'Not set')}
👤 Username: {self.credentials.get('username', 'Not set')}
🔒 Password: {'***' if self.credentials.get('password') else 'Not set'}
🪣 Bucket: {self.credentials.get('bucket_name', 'Not set')}
📖 Read-Only Mode: {self.credentials.get('read_only_mode', 'true')}
        """
    
    def disconnect(self):
        """Disconnect from CouchBase"""
        if self.cluster:
            self.cluster.disconnect()
            self.is_connected = False
            self.connection_healthy = False
            self.connection_status = "disconnected"

    async def call_mcp_tool(self, tool_name: str, args: dict) -> str:
        """
        Call a tool on the Couchbase MCP server.
        """
        mcp_url = os.environ.get("COUCHBASE_MCP_SERVER_URL", "http://localhost:8080")
        endpoint = f"{mcp_url}/tool/{tool_name}"
        async with httpx.AsyncClient() as client:
            response = await client.post(endpoint, json=args)
            response.raise_for_status()
            return response.text

# Global CouchBase client
cb_client = None

def initialize_cb_client():
    """Initialize the robust CouchBase client"""
    global cb_client
    cb_client = RobustCouchbaseClient()
    return cb_client

# Couchbase tool functions (these would be called by Gemini through function calling)
async def get_document_by_id(document_id: str, scope: str = "inventory", collection: str = "airline") -> str:
    """Get a document by ID from Couchbase via MCP server."""
    if not cb_client:
        return json.dumps({"success": False, "error": "CouchBase client not initialized"})
    return await cb_client.call_mcp_tool("get_document_by_id", {
        "document_id": document_id,
        "scope": scope,
        "collection": collection
    })

async def run_query(query: str) -> str:
    """Run a N1QL query on Couchbase via MCP server."""
    if not cb_client:
        return json.dumps({"success": False, "error": "CouchBase client not initialized"})
    return await cb_client.call_mcp_tool("run_query", {"query": query})

async def upsert_document_by_id(document_id: str, document: str, scope: str = "inventory", collection: str = "airline") -> str:
    """Upsert (insert or update) a document by ID in Couchbase via MCP server."""
    if not cb_client:
        return json.dumps({"success": False, "error": "CouchBase client not initialized"})
    
    # Parse document string to dict if needed
    try:
        if isinstance(document, str):
            document_dict = json.loads(document)
        else:
            document_dict = document
    except json.JSONDecodeError:
        return json.dumps({"success": False, "error": "Invalid JSON document format"})
    
    return await cb_client.call_mcp_tool("upsert_document_by_id", {
        "document_id": document_id,
        "document": document_dict,
        "scope": scope,
        "collection": collection
    })

async def delete_document_by_id(document_id: str, scope: str = "inventory", collection: str = "airline") -> str:
    """Delete a document by ID from Couchbase via MCP server."""
    if not cb_client:
        return json.dumps({"success": False, "error": "CouchBase client not initialized"})
    return await cb_client.call_mcp_tool("delete_document_by_id", {
        "document_id": document_id,
        "scope": scope,
        "collection": collection
    })

# Gemini function definitions - using correct schema format
def create_gemini_functions():
    """Create function declarations compatible with Gemini"""
    
    get_document_function = genai.protos.FunctionDeclaration(
        name="get_document_by_id",
        description="Get a document by ID from Couchbase",
        parameters=genai.protos.Schema(
            type=genai.protos.Type.OBJECT,
            properties={
                "document_id": genai.protos.Schema(
                    type=genai.protos.Type.STRING,
                    description="The ID of the document to retrieve"
                ),
                "scope": genai.protos.Schema(
                    type=genai.protos.Type.STRING,
                    description="The scope name (default: inventory)"
                ),
                "collection": genai.protos.Schema(
                    type=genai.protos.Type.STRING,
                    description="The collection name (default: airline)"
                )
            },
            required=["document_id"]
        )
    )
    
    run_query_function = genai.protos.FunctionDeclaration(
        name="run_query",
        description="Run a N1QL query on Couchbase",
        parameters=genai.protos.Schema(
            type=genai.protos.Type.OBJECT,
            properties={
                "query": genai.protos.Schema(
                    type=genai.protos.Type.STRING,
                    description="The N1QL query to execute"
                )
            },
            required=["query"]
        )
    )
    
    upsert_document_function = genai.protos.FunctionDeclaration(
        name="upsert_document_by_id",
        description="Insert or update a document by ID in Couchbase",
        parameters=genai.protos.Schema(
            type=genai.protos.Type.OBJECT,
            properties={
                "document_id": genai.protos.Schema(
                    type=genai.protos.Type.STRING,
                    description="The ID of the document to upsert"
                ),
                "document": genai.protos.Schema(
                    type=genai.protos.Type.STRING,
                    description="The document data as JSON string"
                ),
                "scope": genai.protos.Schema(
                    type=genai.protos.Type.STRING,
                    description="The scope name (default: inventory)"
                ),
                "collection": genai.protos.Schema(
                    type=genai.protos.Type.STRING,
                    description="The collection name (default: airline)"
                )
            },
            required=["document_id", "document"]
        )
    )
    
    delete_document_function = genai.protos.FunctionDeclaration(
        name="delete_document_by_id",
        description="Delete a document by ID from Couchbase",
        parameters=genai.protos.Schema(
            type=genai.protos.Type.OBJECT,
            properties={
                "document_id": genai.protos.Schema(
                    type=genai.protos.Type.STRING,
                    description="The ID of the document to delete"
                ),
                "scope": genai.protos.Schema(
                    type=genai.protos.Type.STRING,
                    description="The scope name (default: inventory)"
                ),
                "collection": genai.protos.Schema(
                    type=genai.protos.Type.STRING,
                    description="The collection name (default: airline)"
                )
            },
            required=["document_id"]
        )
    )
    
    return [
        genai.protos.Tool(function_declarations=[get_document_function]),
        genai.protos.Tool(function_declarations=[run_query_function]),
        genai.protos.Tool(function_declarations=[upsert_document_function]),
        genai.protos.Tool(function_declarations=[delete_document_function])
    ]

# Define a clear system prompt for the agent
COUCHBASE_SYSTEM_PROMPT = """
You are a Couchbase database assistant powered by Google Gemini. You can help users with CRUD operations on their Couchbase database.

Available functions:
- get_document_by_id: Retrieve a document by its ID
- run_query: Execute N1QL queries for complex data retrieval
- upsert_document_by_id: Insert new documents or update existing ones
- delete_document_by_id: Delete documents by their ID

Guidelines:
1. Always ask for clarification if the user request is ambiguous
2. Provide clear explanations of what operations you're performing
3. Handle errors gracefully and suggest solutions
4. Use appropriate N1QL syntax for queries
5. Validate data before performing upsert operations

When users ask about their data, help them understand:
- Database structure and schema
- Available collections and scopes
- Data relationships and patterns
- Best practices for queries
"""

class RobustCouchbaseAgent:
    """Robust CouchBase agent with Gemini AI"""
    
    def __init__(
        self, 
        model_name: str = "gemini-1.5-pro",
        gemini_api_key: Optional[str] = None
    ):
        api_key = gemini_api_key or os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise ValueError("GEMINI_API_KEY must be provided")
        
        # Configure Gemini
        genai.configure(api_key=api_key)
        
        # Initialize the model with function calling - using simpler approach
        self.model = genai.GenerativeModel(
            model_name=model_name,
            system_instruction=COUCHBASE_SYSTEM_PROMPT
        )
        
        # Configure safety settings
        self.safety_settings = {
            HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
            HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
            HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
            HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
        }
        
        # Generation config
        self.generation_config = genai.types.GenerationConfig(
            temperature=0.1,
            max_output_tokens=4096,
            response_mime_type="text/plain"
        )
        
        initialize_cb_client()
        
        # Available tools mapping
        self.available_tools = {
            "get_document_by_id": get_document_by_id,
            "run_query": run_query,
            "upsert_document_by_id": upsert_document_by_id,
            "delete_document_by_id": delete_document_by_id
        }
        
        # Start without chat session initially - we'll use generate_content_async
        # self.chat = self.model.start_chat(history=[])
    
    async def connect_to_database(self) -> bool:
        """Connect to database with robust error handling"""
        if cb_client:
            return await cb_client.connect_with_robust_retry()
        return False
    
    async def process_query(self, user_query: str) -> str:
        """Process queries with Gemini using manual function calling"""
        try:
            print(f"🔍 Processing query with Gemini: {user_query}")
            
            # First, let Gemini understand the request and suggest actions
            analysis_prompt = f"""
            User request: "{user_query}"
            
            You are a Couchbase database assistant. Analyze this request and determine what database operations are needed.
            
            Available operations:
            1. get_document_by_id - Get a specific document by ID
            2. run_query - Execute N1QL queries for data retrieval  
            3. upsert_document_by_id - Create or update documents
            4. delete_document_by_id - Delete documents
            
            If this is a:
            - Data retrieval request: Use run_query with appropriate N1QL
            - Get specific document: Use get_document_by_id 
            - Create/update data: Use upsert_document_by_id
            - Delete request: Use delete_document_by_id
            - Diagnostic request: Use run_query to check system information
            
            Respond with:
            1. Your understanding of the request
            2. What operation(s) you would perform
            3. The specific function call needed (with parameters)
            
            If you need to execute a function, format it as:
            FUNCTION_CALL: function_name(param1="value1", param2="value2")
            """
            
            response = await self.model.generate_content_async(
                analysis_prompt,
                generation_config=self.generation_config,
                safety_settings=self.safety_settings
            )
            
            response_text = response.text
            print(f"🧠 Gemini analysis: {response_text}")
            
            # Check if Gemini wants to call a function
            if "FUNCTION_CALL:" in response_text:
                # Extract function call
                function_line = [line for line in response_text.split('\n') if 'FUNCTION_CALL:' in line][0]
                function_call = function_line.split('FUNCTION_CALL:')[1].strip()
                
                # Parse function name and parameters
                import re
                match = re.match(r'(\w+)\((.*)\)', function_call)
                if match:
                    function_name = match.group(1)
                    params_str = match.group(2)
                    
                    print(f"🛠️ Executing function: {function_name}")
                    
                    # Parse parameters
                    function_args = {}
                    if params_str:
                        # Simple parameter parsing (you might want to make this more robust)
                        param_pairs = params_str.split(',')
                        for pair in param_pairs:
                            if '=' in pair:
                                key, value = pair.split('=', 1)
                                key = key.strip().strip('"\'')
                                value = value.strip().strip('"\'')
                                function_args[key] = value
                    
                    # Execute the function
                    if function_name in self.available_tools:
                        try:
                            tool_func = self.available_tools[function_name]
                            result = await tool_func(**function_args)
                            
                            print(f"✅ Function {function_name} completed")
                            
                            # Get Gemini to interpret the results
                            interpretation_prompt = f"""
                            Original user request: "{user_query}"
                            Function executed: {function_name}
                            Function result: {result}
                            
                            Please provide a helpful response to the user explaining:
                            1. What you found or did
                            2. The results in a user-friendly format
                            3. Any insights or recommendations
                            4. Next steps if applicable
                            
                            Be conversational and helpful.
                            """
                            
                            final_response = await self.model.generate_content_async(
                                interpretation_prompt,
                                generation_config=self.generation_config,
                                safety_settings=self.safety_settings
                            )
                            
                            return final_response.text
                            
                        except Exception as e:
                            error_msg = f"Error executing {function_name}: {str(e)}"
                            print(f"❌ Function {function_name} failed: {e}")
                            
                            # Let Gemini handle the error
                            error_prompt = f"""
                            User request: "{user_query}"
                            Attempted function: {function_name}
                            Error: {error_msg}
                            
                            Explain this error to the user in a helpful way and suggest alternatives or troubleshooting steps.
                            """
                            
                            error_response = await self.model.generate_content_async(
                                error_prompt,
                                generation_config=self.generation_config,
                                safety_settings=self.safety_settings
                            )
                            
                            return error_response.text
                    else:
                        return f"❌ Unknown function requested: {function_name}"
            
            # If no function call needed, return the analysis
            return response_text
            
        except Exception as e:
            logger.error(f"Error processing query with Gemini: {e}")
            return f"❌ I encountered an error: {str(e)}\n\nLet me help you troubleshoot this issue. Try asking me to 'diagnose the connection' to identify any problems."
    
    async def cleanup(self):
        """Cleanup resources"""
        if cb_client:
            cb_client.disconnect()

# Enhanced main function
async def main():
    """Enhanced main with robust error handling and Gemini integration"""
    
    print("🛡️ Starting Robust CouchBase Agent with Gemini AI")
    print("=" * 70)
    
    try:
        agent = RobustCouchbaseAgent(
            model_name=os.getenv("GEMINI_MODEL", "gemini-1.5-pro"),
            gemini_api_key=os.getenv("GEMINI_API_KEY")
        )
        print("✅ Gemini agent initialized successfully")
    except ValueError as e:
        print(f"❌ Configuration error: {e}")
        print("\nPlease set your GEMINI_API_KEY environment variable:")
        print("export GEMINI_API_KEY='your_api_key_here'")
        return
    
    # Validate credentials
    credentials_valid, message = cb_client.validate_credentials()
    
    if not credentials_valid:
        print("❌ Missing CouchBase credentials!")
        print(f"\nError: {message}")
        return
    
    print("✅ Credentials validated")
    print("📋 Credential Summary:")
    print(cb_client.get_credentials_summary())
    
    # Robust connection attempt
    print("\n🔌 Attempting robust connection...")
    connected = await agent.connect_to_database()
    
    if connected:
        print("✅ Connected successfully!")
    else:
        print("⚠️ Connection issues detected, but continuing with diagnostics...")
    
    # Start with diagnostic queries
    diagnostic_queries = [
        "diagnose my connection and tell me what's happening",
        "explain about the data in my database"
    ]
    
    bucket_name = cb_client.credentials["bucket_name"]
    print(f"\n🔍 Running diagnostics for database: {bucket_name}")
    print("=" * 70)
    
    for i, query in enumerate(diagnostic_queries, 1):
        print(f"\n📝 Diagnostic {i}: {query}")
        print("-" * 70)
        
        try:
            response = await agent.process_query(query)
            print(f"🤖 Gemini Agent Response:\n{response}")
        except Exception as e:
            print(f"❌ Error: {e}")
        
        print("\n" + "=" * 70)
        
        # Add delay between diagnostics
        await asyncio.sleep(2)
    
    # Interactive mode with robust error handling
    print(f"\n🎯 Interactive Gemini Mode")
    print("The Gemini-powered agent will help diagnose and resolve any connection or data issues.")
    print("\nSuggested commands:")
    print("  • 'show me all documents in the airline collection'")
    print("  • 'create a new customer with name John and email john@example.com'")
    print("  • 'find all documents where status is active'")
    print("  • 'update document customer123 with new phone number'")
    print("  • 'delete document with ID old_record'")
    print("  • 'diagnose my connection'")
    print("  • 'what data do I have?'")
    print("Type 'quit' to exit")
    print("-" * 70)
    
    while True:
        try:
            user_query = input("\n🎯 Your query: ").strip()
            if user_query.lower() in ['quit', 'exit', 'bye', 'q']:
                break
            
            if user_query:
                response = await agent.process_query(user_query)
                print(f"\n🤖 Gemini Assistant: {response}")
                
        except (EOFError, KeyboardInterrupt):
            break
        except Exception as e:
            print(f"\n❌ Unexpected error: {e}")
            print("The agent will continue running. Try asking for help with diagnostics.")

    print("\n🛑 Shutting down Gemini agent...")
    await agent.cleanup()
    print("✅ Cleanup completed. Thank you for using the Gemini-powered agent! 👋")

if __name__ == "__main__":
    # Install required packages note
    print("📦 Required packages:")
    print("pip install google-generativeai couchbase httpx")
    print("\n🔑 Don't forget to set your GEMINI_API_KEY!")
    print("Get your API key from: AIzaSyA5FtbbhjQB8sGgT6kvWmtLm-deyYhmrLQ")
    print("=" * 70)
    
    asyncio.run(main())