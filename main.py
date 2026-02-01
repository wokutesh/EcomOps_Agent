import uuid
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import os
import asyncio
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from groq import Groq

load_dotenv()

# Initialize Groq - using the key name from your snippet
groq_client = Groq(api_key=os.getenv("groq_api"))

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- MODELS ---
class ManagerConfig(BaseModel):
    full_name: str
    company_name: str
    db_host: str
    db_user: str
    db_pass: str
    db_name: str
    db_port: int

class EcomOpsRequest(BaseModel):
    prompt: str
    conversation_id: Optional[str] = None
    manager_id: str 
    agent_id: str = "ecomops_master"

# --- DB CONNECTION ---
def get_master_db_conn():
    try:
        conn = psycopg2.connect(
            host="aws-1-eu-west-1.pooler.supabase.com", 
            database="postgres",
            user="postgres.ghkglgpzemroczxddbul",
            password="@A1g2e3n4t5", 
            port=6543,
            sslmode='require',
            options="-c pgbouncer=true"
        )
        return conn
    except Exception as e:
        print(f"❌ Could not connect to Master DB: {e}")
        raise e

# --- CORE LOGIC ---
async def call_mcp_agent(user_prompt, db_creds):
    server_params = StdioServerParameters(command="python", args=["server.py"])
    
    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # The System Prompt tells the agent who it is and what tools it has
            agent_instructions = (
                "You are the EcomOps DB Manager. "
                "If a developer asks about structure, use 'inspect_schema'. "
                "If a manager asks about sales/activity, use 'track_activity'. "
                "For specific questions, use 'execute_sql' after generating the correct query."
            )

            # Let Groq decide which tool to call based on the prompt
            # (Note: This requires Groq's tool_calling capability or a logic gate)
            
            # For simplicity, let's stick to your current 'execute_sql' flow but 
            # improve the SQL generation to be aware of the schema.
            
            # 1. First, the agent 'Inspects' if it doesn't know the schema
            schema_info = await session.call_tool("inspect_schema", arguments={
                "host": db_creds['db_host'], "user": db_creds['db_user'], 
                "password": db_creds['db_pass'], "dbname": db_creds['db_name'], "port": db_creds['db_port']
            })

            # 2. Now Groq writes SQL knowing the EXACT table names
            sql_gen = groq_client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {"role": "system", "content": f"Schema: {schema_info.content[0].text}\nOutput raw SQL only."},
                    {"role": "user", "content": user_prompt}
                ]
            )
            clean_sql = sql_gen.choices[0].message.content.replace("```sql", "").replace("```", "").strip()

            # 3. Execute
            final_data = await session.call_tool("execute_sql", arguments={
                "sql_query": clean_sql,
                "host": db_creds['db_host'], "user": db_creds['db_user'], 
                "password": db_creds['db_pass'], "dbname": db_creds['db_name'], "port": db_creds['db_port']
            })
            
            return final_data.content[0].text

@app.post("/api/v1/register-manager")
async def register_manager(config: ManagerConfig):
    conn = None
    try:
        conn = get_master_db_conn()
        cursor = conn.cursor()
        query = """
            INSERT INTO manager_configs (full_name, company_name, db_host, db_user, db_pass, db_name, db_port)
            VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING manager_id;
        """
        cursor.execute(query, (config.full_name, config.company_name, config.db_host, config.db_user, config.db_pass, config.db_name, config.db_port))
        new_id = cursor.fetchone()[0]
        conn.commit()
        return {"status": "success", "manager_id": str(new_id)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn: conn.close()

@app.post("/api/v1/converse") # Fixed: Added missing decorator
async def converse(request: EcomOpsRequest):
    conn = None
    try:
        conn = get_master_db_conn()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # 1. Fetch credentials
        cursor.execute("SELECT * FROM manager_configs WHERE manager_id = %s", (request.manager_id,))
        db_creds = cursor.fetchone()
        if not db_creds:
            raise HTTPException(status_code=404, detail="Config not found")

        # 2. Run MCP + Groq
        ai_message = await call_mcp_agent(request.prompt, db_creds)
        
        # 3. Handle IDs & History
        real_conv_id = request.conversation_id
        if not real_conv_id or real_conv_id.startswith('conv_'):
            real_conv_id = str(uuid.uuid4())

        cursor.execute("SELECT id FROM chat_threads WHERE id = %s", (real_conv_id,))
        if not cursor.fetchone():
            cursor.execute("INSERT INTO chat_threads (id, manager_id, title) VALUES (%s, %s, %s)", 
                           (real_conv_id, request.manager_id, request.prompt[:50]))

        cursor.execute("INSERT INTO chat_messages (conversation_id, role, content) VALUES (%s, %s, %s)", 
                       (real_conv_id, 'user', request.prompt))
        cursor.execute("INSERT INTO chat_messages (conversation_id, role, content) VALUES (%s, %s, %s)", 
                       (real_conv_id, 'assistant', ai_message))
        
        conn.commit()
        return {"conversation_id": real_conv_id, "response": {"message": ai_message}}

    except Exception as e:
        if conn: conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn: conn.close()

# --- HISTORY ROUTES ---
@app.get("/api/v1/history/{conversation_id}")
async def get_chat_history(conversation_id: str):
    conn = get_master_db_conn()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT role, content, created_at FROM chat_messages WHERE conversation_id = %s ORDER BY created_at ASC", (conversation_id,))
    history = cursor.fetchall()
    conn.close()
    return {"history": history}

@app.get("/api/v1/threads/{manager_id}")
async def get_manager_threads(manager_id: str):
    conn = get_master_db_conn()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT id, title, created_at FROM chat_threads WHERE manager_id = %s ORDER BY created_at DESC", (manager_id,))
    threads = cursor.fetchall()
    conn.close()
    return {"threads": threads}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)