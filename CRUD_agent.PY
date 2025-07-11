import os
import json
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text
from typing import Dict, List, Any, Optional
from google.cloud.sql.connector import Connector
from dataclasses import dataclass
from pathlib import Path
import streamlit as st
import socket
from datetime import datetime

# LangGraph imports
from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages
from typing_extensions import Annotated, TypedDict

# LangChain imports
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from langchain_core.prompts import ChatPromptTemplate

# Set up credentials and environment variables
def setup_credentials():
    """Set up Google Cloud credentials and database connection parameters."""
    # Set Google Cloud credentials
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:/Users/HP ZBOOK FURY G7/Downloads/llm-proj-legal-data-c9c44366d8ca.json"
    
    # Set Cloud SQL connection parameters
    os.environ["GCLOUD_CLOUD_SQL_INSTANCE"] = "llm-proj-legal-data:us-central1:aneeshjayan"
    os.environ["DB_USER"] = "root"
    os.environ["DB_PASS"] = "aneesh123#"
    os.environ["DB_NAME"] = "customerdata"

# Remove login, login event logging, and user table logic
# Remove login_user, log_login_event, ensure_user_and_login_tables, and all login/session state code
# Start app directly with database/table selection and question interface

# State definition for the agent
class AgentState(TypedDict):
    messages: Annotated[List[HumanMessage | AIMessage], add_messages]
    user_question: str
    table_name: str
    metadata: Dict[str, Any]
    sql_query: str
    operation_type: str
    result: Any
    error: Optional[str]

class DatabaseCRUDAgent:
    """Main agent class for database CRUD operations using Google Cloud SQL Connector."""
    def __init__(self, google_api_key: str, table_name: str = "excel_data"):
        # Set up credentials first
        setup_credentials()
        
        # Initialize LLM
        self.llm = ChatGoogleGenerativeAI(
            model="gemini-1.5-flash",
            google_api_key=google_api_key,
            temperature=0.1
        )
        # Initialize Cloud SQL Connector and SQLAlchemy pool
        self.connector = Connector()
        self.pool = None
        self.table_name = table_name
        self._setup_database_connection()

    def _setup_database_connection(self):
        """Setup database connection with proper error handling."""
        try:
            # Get connection parameters from environment
            instance_name = os.getenv("GCLOUD_CLOUD_SQL_INSTANCE")
            db_user = os.getenv("DB_USER")
            db_pass = os.getenv("DB_PASS")
            db_name = os.getenv("DB_NAME")
            
            if not all([instance_name, db_user, db_pass, db_name]):
                raise ValueError("Missing required environment variables for Cloud SQL connection")
            
            st.info(f"🔗 Connecting to Cloud SQL instance: {instance_name}")
            
            self.pool = create_engine(
                "mysql+pymysql://",
                creator=lambda: self.connector.connect(
                    instance_name,  # "llm-proj-legal-data:us-central1:aneeshjayan"
                    "pymysql",
                    user=db_user,
                    password=db_pass,
                    db=db_name
                ),
                pool_size=5,
                max_overflow=0,
                pool_pre_ping=True
            )
            
            # Test the connection
            with self.pool.connect() as conn:
                conn.execute(text("SELECT 1"))
                st.success("✅ Database connection established successfully!")
                
        except Exception as e:
            st.error(f"❌ Database connection failed: {str(e)}")
            st.error("Please check your Google Cloud credentials and database configuration")
            raise

    def get_available_tables(self) -> List[str]:
        """Get list of available tables in the database."""
        try:
            with self.pool.connect() as conn:
                result = conn.execute(text("SHOW TABLES"))
                tables = [row[0] for row in result]
                return tables
        except Exception as e:
            st.error(f"❌ Failed to get tables: {str(e)}")
            return []

    def get_table_metadata(self, table_name: str) -> Dict[str, Any]:
        """Extract metadata from the database table."""
        try:
            # Get table structure
            with self.pool.connect() as conn:
                # Get column information
                columns_query = f"DESCRIBE {table_name}"
                columns_result = conn.execute(text(columns_query))
                columns_info = [dict(row._asdict()) for row in columns_result]
                
                # Get sample data
                sample_query = f"SELECT * FROM {table_name} LIMIT 5"
                sample_df = pd.read_sql_query(sample_query, self.pool)
                
                # Get total count
                count_query = f"SELECT COUNT(*) as total_count FROM {table_name}"
                count_result = conn.execute(text(count_query))
                total_rows = count_result.scalar()
                
                # Get data types and basic stats
                df_sample = pd.read_sql_query(f"SELECT * FROM {table_name} LIMIT 1000", self.pool)
                
            metadata = {
                "table_info": {
                    "table_name": table_name,
                    "total_rows": total_rows,
                    "total_columns": len(columns_info),
                    "column_info": columns_info
                },
                "data_info": {
                    "column_names": [col['Field'] for col in columns_info],
                    "data_types": {col['Field']: col['Type'] for col in columns_info},
                    "sample_data": sample_df.to_dict('records') if not sample_df.empty else []
                },
                "statistics": {
                    "numeric_columns": df_sample.select_dtypes(include=['number']).columns.tolist(),
                    "string_columns": df_sample.select_dtypes(include=['object']).columns.tolist(),
                    "date_columns": df_sample.select_dtypes(include=['datetime']).columns.tolist()
                }
            }
            return metadata
        except Exception as e:
            st.error(f"❌ Failed to get table metadata: {str(e)}")
            raise

    def execute_sql_query(self, sql_query: str) -> Dict[str, Any]:
        """Execute SQL query on the Cloud SQL database."""
        try:
            st.info(f"🔍 Executing SQL query: {sql_query}")
            
            if sql_query.strip().upper().startswith('SELECT'):
                df = pd.read_sql_query(sql_query, self.pool)
                return {
                    "success": True, 
                    "data": df.to_dict('records'), 
                    "rows_affected": len(df), 
                    "query_type": "SELECT",
                    "dataframe": df
                }
            else:
                with self.pool.begin() as conn:
                    result = conn.execute(text(sql_query))
                    return {
                        "success": True, 
                        "message": f"Query executed successfully. Rows affected: {result.rowcount}", 
                        "rows_affected": result.rowcount, 
                        "query_type": "MODIFY"
                    }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def close_connection(self):
        """Clean up database connections."""
        try:
            if self.pool:
                self.pool.dispose()
            if self.connector:
                self.connector.close()
        except Exception as e:
            st.warning(f"⚠️ Error closing connections: {str(e)}")


class DatabaseCRUDWorkflow:
    """Main workflow class using LangGraph for database operations."""
    def __init__(self, google_api_key: str, table_name: str = "excel_data"):
        self.agent = DatabaseCRUDAgent(google_api_key, table_name)
        self.workflow = None
        self.build_workflow()

    def build_workflow(self):
        workflow = StateGraph(AgentState)
        workflow.add_node("metadata_analyzer", self.metadata_analyzer_node)
        workflow.add_node("sql_generator", self.sql_generator_node)
        workflow.add_node("sql_executor", self.sql_executor_node)
        workflow.add_node("response_generator", self.response_generator_node)
        workflow.set_entry_point("metadata_analyzer")
        workflow.add_edge("metadata_analyzer", "sql_generator")
        workflow.add_edge("sql_generator", "sql_executor")
        workflow.add_edge("sql_executor", "response_generator")
        workflow.add_edge("response_generator", END)
        self.workflow = workflow.compile()

    def metadata_analyzer_node(self, state: AgentState) -> AgentState:
        try:
            # Get metadata from existing database table
            metadata = self.agent.get_table_metadata(state["table_name"])
            
            prompt = ChatPromptTemplate.from_messages([
                ("system", """
You are a data analyst. Based on the user question and table metadata, determine the operation type.
Operation types: CREATE, READ, UPDATE, DELETE, ANALYZE

Table Metadata: {metadata}

Return only the operation type (one word)."""),
                ("human", "{user_question}")
            ])
            
            chain = prompt | self.agent.llm
            response = chain.invoke({
                "metadata": json.dumps(metadata, indent=2),
                "user_question": state["user_question"]
            })
            
            state["metadata"] = metadata
            state["operation_type"] = response.content.strip().upper()
            return state
        except Exception as e:
            state["error"] = str(e)
            return state

    def sql_generator_node(self, state: AgentState) -> AgentState:
        try:
            cols = ", ".join(state["metadata"]["data_info"]["column_names"])
            sample = json.dumps(state["metadata"]["data_info"]["sample_data"], indent=2)
            
            prompt = ChatPromptTemplate.from_messages([
                ("system", """
You are a SQL expert. Generate a SQL query based on the user question and table metadata.

Table name: {table_name}
Columns: {columns}
Sample data: {sample_data}
Operation type: {operation_type}

Generate ONLY the SQL query without any explanation or markdown formatting.
Use proper SQL syntax for MySQL/MariaDB."""),
                ("human", "{user_question}")
            ])
            
            chain = prompt | self.agent.llm
            res = chain.invoke({
                "table_name": state["table_name"],
                "columns": cols,
                "sample_data": sample,
                "operation_type": state["operation_type"],
                "user_question": state["user_question"]
            })
            
            # Clean the SQL query
            query = res.content.strip()
            if query.startswith('```sql'):
                query = query[6:]
            if query.endswith('```'):
                query = query[:-3]
            query = query.strip().strip('"').strip("'")
            
            state["sql_query"] = query
            return state
        except Exception as e:
            state["error"] = str(e)
            return state

    def sql_executor_node(self, state: AgentState) -> AgentState:
        try:
            state["result"] = self.agent.execute_sql_query(state["sql_query"])
            return state
        except Exception as e:
            state["error"] = str(e)
            return state

    def response_generator_node(self, state: AgentState) -> AgentState:
        try:
            if state.get("error"):
                resp = f"❌ Error: {state['error']}"
            else:
                res = state["result"]
                if res["success"]:
                    if res.get("data"):
                        resp_text = f"✅ Query executed successfully. Found {len(res['data'])} records."
                    else:
                        resp_text = res.get("message", "✅ Operation completed successfully.")
                else:
                    resp_text = f"❌ Query failed: {res.get('error')}"
                
                prompt = ChatPromptTemplate.from_messages([
                    ("system", """
You are a helpful assistant. Generate a clear, user-friendly response based on:
User question: {user_question}
SQL Query used: {sql_query}
Result: {outcome}

Provide a natural language explanation of what was done and the results."""),
                    ("human", "Generate response")
                ])
                
                chain = prompt | self.agent.llm
                out = chain.invoke({
                    "user_question": state["user_question"],
                    "sql_query": state["sql_query"],
                    "outcome": resp_text
                })
                resp = out.content
                
            state["messages"] = state["messages"] + [AIMessage(content=resp)]
            return state
        except Exception as e:
            state["error"] = str(e)
            state["messages"] = state["messages"] + [AIMessage(content=f"❌ Error generating response: {str(e)}")]
            return state

    def run(self, user_question: str, table_name: str) -> tuple[str, Dict[str, Any]]:
        init = {
            "messages": [HumanMessage(content=user_question)],
            "user_question": user_question,
            "table_name": table_name,
            "metadata": {},
            "sql_query": "",
            "operation_type": "",
            "result": None,
            "error": None
        }
        try:
            final = self.workflow.invoke(init)
            if final.get("error"):
                return f"❌ Error: {final['error']}", {}
            return final["messages"][-1].content, final.get("result", {})
        except Exception as e:
            return f"❌ Workflow error: {str(e)}", {}

    def cleanup(self):
        """Clean up resources."""
        self.agent.close_connection()


# Streamlit app
def main():
    st.set_page_config(page_title="Database Chatbot", layout="wide")
    st.markdown("""
        <style>
        .chat-header {
            font-size: 2.2rem;
            font-weight: 700;
            display: flex;
            align-items: center;
            gap: 0.7em;
            margin-bottom: 0.5em;
            animation: fadeInDown 1s;
        }
        .bot-avatar {
            width: 48px; height: 48px; border-radius: 50%; background: #f0f4ff; display: flex; align-items: center; justify-content: center; font-size: 2rem; animation: pulse 1.5s infinite;
        }
        @keyframes fadeInDown {
            from { opacity: 0; transform: translateY(-30px); }
            to { opacity: 1; transform: translateY(0); }
        }
        @keyframes pulse {
            0% { box-shadow: 0 0 0 0 #4f8cff44; }
            70% { box-shadow: 0 0 0 10px #4f8cff00; }
            100% { box-shadow: 0 0 0 0 #4f8cff00; }
        }
        .typing-dots {
            display: inline-block; width: 2em; text-align: left;
        }
        .typing-dots span {
            display: inline-block; width: 0.4em; height: 0.4em; margin-right: 0.1em; background: #4f8cff; border-radius: 50%; opacity: 0.6; animation: blink 1.4s infinite both;
        }
        .typing-dots span:nth-child(2) { animation-delay: 0.2s; }
        .typing-dots span:nth-child(3) { animation-delay: 0.4s; }
        @keyframes blink {
            0%, 80%, 100% { opacity: 0.6; }
            40% { opacity: 1; }
        }
        </style>
    """, unsafe_allow_html=True)

    st.markdown('<div class="chat-header"><div class="bot-avatar">🤖</div>Database Chatbot</div>', unsafe_allow_html=True)
    st.sidebar.header("🔑 Configuration")
    google_api_key = st.sidebar.text_input("Google API Key", type="password")
    st.sidebar.subheader("📋 Environment Status")
    setup_credentials()
    env_vars = ["GCLOUD_CLOUD_SQL_INSTANCE", "DB_USER", "DB_PASS", "DB_NAME", "GOOGLE_APPLICATION_CREDENTIALS"]
    for var in env_vars:
        if os.getenv(var):
            st.sidebar.success(f"✅ {var} is set")
        else:
            st.sidebar.error(f"❌ {var} is missing")
    st.sidebar.subheader("🔗 Connection Details")
    st.sidebar.info(f"**Instance**: {os.getenv('GCLOUD_CLOUD_SQL_INSTANCE')}")
    st.sidebar.info(f"**Database**: {os.getenv('DB_NAME')}")
    st.sidebar.info(f"**User**: {os.getenv('DB_USER')}")
    if not google_api_key:
        st.sidebar.warning("⚠️ Please enter your Google API key to proceed.")
        st.warning("🔑 **Required**: Please enter your Google API key in the sidebar to use this application.")
        return
    # Table selection
    try:
        with st.spinner("🔄 Connecting to database..."):
            temp_agent = DatabaseCRUDAgent(google_api_key)
            available_tables = temp_agent.get_available_tables()
            temp_agent.close_connection()
        if not available_tables:
            st.error("❌ No tables found in the database.")
            return
        st.sidebar.success(f"✅ Connected to database! Found {len(available_tables)} tables.")
    except Exception as e:
        st.sidebar.error(f"❌ Failed to connect to database: {str(e)}")
        return
    st.sidebar.subheader("📊 Select Table")
    selected_table = st.sidebar.selectbox(
        "Choose a table to work with:",
        available_tables,
        help="Select the database table you want to query"
    )
    if not selected_table:
        st.info("📋 Please select a table to proceed.")
        return
    try:
        with st.spinner("🔄 Initializing workflow..."):
            workflow = DatabaseCRUDWorkflow(google_api_key, selected_table)
        st.sidebar.success("✅ Workflow initialized successfully!")
    except Exception as e:
        st.sidebar.error(f"❌ Failed to initialize workflow: {str(e)}")
        return
    # Table preview in sidebar
    try:
        with st.spinner("📋 Loading table preview..."):
            preview_df = pd.read_sql_query(f"SELECT * FROM {selected_table} LIMIT 10", workflow.agent.pool)
        st.sidebar.subheader("📋 Table Preview")
        st.sidebar.dataframe(preview_df, use_container_width=True)
        with workflow.agent.pool.connect() as conn:
            total_count = conn.execute(text(f"SELECT COUNT(*) FROM {selected_table}")).scalar()
        st.sidebar.info(f"📊 Table: {selected_table} | Total Records: {total_count:,} | Columns: {len(preview_df.columns)}")
        with st.sidebar.expander("📝 Column Information"):
            col_info = pd.DataFrame({
                'Column': preview_df.columns,
                'Type': preview_df.dtypes.values,
                'Sample Value': [str(preview_df[col].iloc[0]) if not preview_df.empty else 'N/A' for col in preview_df.columns]
            })
            st.dataframe(col_info, use_container_width=True)
    except Exception as e:
        st.sidebar.error(f"❌ Failed to load table preview: {str(e)}")
    # --- Chatbot UI ---
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []
    # Display chat history
    for msg in st.session_state.chat_history:
        if msg["role"] == "user":
            with st.chat_message("user"):
                st.markdown(msg["content"])
        else:
            with st.chat_message("assistant"):
                st.markdown(msg["content"])
    # Chat input
    user_input = st.chat_input("Type your question about the data and press Enter...")
    if user_input:
        st.session_state.chat_history.append({"role": "user", "content": user_input})
        with st.chat_message("user"):
            st.markdown(user_input)
        # Show animated typing dots while bot is thinking
        with st.chat_message("assistant"):
            st.markdown('<div class="typing-dots"><span></span><span></span><span></span></div>', unsafe_allow_html=True)
        # Process the question (human-in-the-loop confirmation)
        try:
            with st.spinner("🔄 Processing your question..."):
                # Run metadata analyzer only
                workflow.agent = DatabaseCRUDAgent(google_api_key, selected_table)
                metadata = workflow.agent.get_table_metadata(selected_table)
                prompt = ChatPromptTemplate.from_messages([
                    ("system", """
You are a data analyst. Based on the user question and table metadata, determine the operation type.
Operation types: CREATE, READ, UPDATE, DELETE, ANALYZE
Table Metadata: {metadata}
Return only the operation type (one word)."""),
                    ("human", "{user_question}")
                ])
                chain = prompt | workflow.agent.llm
                response = chain.invoke({
                    "metadata": json.dumps(metadata, indent=2),
                    "user_question": user_input
                })
                op_type = response.content.strip().upper()
                st.session_state.pending_metadata = metadata
                st.session_state.pending_operation_type = op_type
            # Show detected operation and metadata, ask for confirmation
            with st.chat_message("assistant"):
                st.markdown(f"**Detected operation type:** `{op_type}`")
                st.json(metadata)
                st.warning("Please confirm: Do you want to proceed with this operation?")
                confirm = st.radio("Proceed?", ["Yes", "No", "Modify"], key=f"confirm_op_{len(st.session_state.chat_history)}")
                if confirm == "Yes":
                    response, result = workflow.run(user_input, selected_table)
                    st.session_state.chat_history.append({"role": "assistant", "content": response})
                    st.markdown(response)
                    if result.get("success") and result.get("data"):
                        st.subheader("📊 Query Results")
                        res_df = pd.DataFrame(result['data'])
                        st.dataframe(res_df)
                        st.info(f"📈 Results: {len(res_df)} rows × {len(res_df.columns)} columns")
                        csv = res_df.to_csv(index=False)
                        st.download_button(
                            label="📥 Download Results as CSV",
                            data=csv,
                            file_name=f"{selected_table}_query_results.csv",
                            mime="text/csv"
                        )
                        from io import BytesIO
                        excel_buffer = BytesIO()
                        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                            res_df.to_excel(writer, sheet_name='Results', index=False)
                        excel_buffer.seek(0)
                        st.download_button(
                            label="📥 Download Results as Excel",
                            data=excel_buffer,
                            file_name=f"{selected_table}_query_results.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                    elif result.get("success"):
                        st.success("✅ Operation completed successfully!")
                        if result.get("message"):
                            st.info(result["message"])
                elif confirm == "No":
                    st.session_state.chat_history.append({"role": "assistant", "content": "Operation cancelled by user."})
                    st.info("Operation cancelled by user.")
                elif confirm == "Modify":
                    st.session_state.chat_history.append({"role": "assistant", "content": "Please modify your question and try again."})
                    st.info("Please modify your question and try again.")
        except Exception as e:
            with st.chat_message("assistant"):
                st.session_state.chat_history.append({"role": "assistant", "content": f"❌ Error processing question: {str(e)}"})
                st.error(f"❌ Error processing question: {str(e)}")
        finally:
            try:
                workflow.cleanup()
            except:
                pass

if __name__ == "__main__":
    main()