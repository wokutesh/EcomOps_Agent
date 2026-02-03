import re
import uuid
from fastapi import FastAPI, HTTPException,Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel,EmailStr,Field,validator
from typing import Optional, Dict
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import os
from passlib.context import CryptContext
import asyncio
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from groq import Groq

load_dotenv()
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
# Initialize Groq - using the key name from your snippet
groq_client = Groq(api_key=os.getenv("groq_api"))

app = FastAPI()

SYSTEM_PROMPT = """
You are the EcomOps Command Agent, a strategic database analyst. 
Your goal is to transform raw database tuples into insightful business intelligence.

### RESPONSE RULES:
1. MULTI-TOOL VERIFICATION: Use at least two tools if the query requires context (e.g., performance + active connections).
2. NO RAW CODE: Never display raw Python lists, tuples, or brackets like '[(...)]'. 
3. DATA FORMATTING: Always convert list data into clean Markdown Tables.
4. TONE: Professional, concise, and helpful.

### STRUCTURE:
- STATUS: Start with 🟢 (Healthy), 🟡 (Warning), or 🔴 (Critical).
- ANALYSIS: Explain 'the why' behind the numbers.
- DATA TABLE: A formatted table of the results.
- RECOMMENDATION: Provide a concrete next step for the manager.

### TRUTHFULNESS:
If a tool returns no data, state "No records found for this period." Never hallucinate numbers.
"""

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
    email: EmailStr  
    password: str = Field(..., min_length=8, max_length=72)
    db_host: str
    db_user: str
    db_pass: str
    db_name: str
    db_port: int
    
    @validator('password')
    def password_strength_check(cls, v):
        if not re.search(r'[A-Z]', v):
            raise ValueError('Password must contain at least one uppercase letter')
        if not re.search(r'[a-z]', v):
            raise ValueError('Password must contain at least one lowercase letter')
        if not re.search(r'[0-9]', v):
            raise ValueError('Password must contain at least one number')
        if not re.search(r'[!@#$%^&*(),.?":{}|<>]', v):
            raise ValueError('Password must contain at least one special character')
        return v

class EcomOpsRequest(BaseModel):
    prompt: str
    conversation_id: Optional[str] = None
    manager_id: str 
    agent_id: str = "ecomops_master"

class LoginSchema(BaseModel):
    email: EmailStr
    password: str

class PasswordUpdate(BaseModel):
    manager_id: str
    new_password: str = Field(..., max_length=72)
# --- PASSWORD UTILS ---
def hash_password(password: str):
    # Truncate to 72 chars to satisfy bcrypt's limit before hashing
    return pwd_context.hash(password[:72]) 

def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password[:72], hashed_password)


def get_master_db_conn():
    try:
        conn = psycopg2.connect(
            host="aws-1-eu-west-1.pooler.supabase.com", 
            database="postgres",
            user="postgres.ghkglgpzemroczxddbul",
            password = "@A1g2e3n4t5", 
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

            # 1. Inspect Schema (so the AI knows the table structure)
            schema_info = await session.call_tool("inspect_schema", arguments={
                    "host": db_creds['db_host'], "user": db_creds['db_user'], 
                    "password": db_creds['db_pass'], "dbname": db_creds['db_name'], "port": db_creds['db_port']
                })

            # 2. Generate the SQL
            sql_gen = groq_client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages = [
                    {"role": "system", "content": f"Use this schema: {schema_info.content[0].text}. If looking for cities, check the 'customers' table for the exact column name."},
                    {"role": "user", "content": user_prompt}
                ]
            )
            clean_sql = sql_gen.choices[0].message.content.replace("```sql", "").replace("```", "").strip()

            # 3. Execute the SQL to get the raw data (the [(3, 'desktop', ...)] part)
            raw_data = await session.call_tool("execute_sql", arguments={
                "sql_query": clean_sql,
                "host": db_creds['db_host'], "user": db_creds['db_user'], 
                "password": db_creds['db_pass'], "dbname": db_creds['db_name'], "port": db_creds['db_port']
            })
            
            if "Error:" in raw_data.content[0].text:
                
                return f"❌ Database"
            final_response = groq_client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT}, 
                    {"role": "user", "content": f"The manager asked: {user_prompt}. Here is the raw DB data: {raw_data.content[0].text}. Summarize it."}
                ]
            )
            
            return final_response.choices[0].message.content

@app.post("/api/v1/register-manager")
async def register_manager(config: ManagerConfig):
    conn = None
    try:
        hashed_pwd = hash_password(config.password)
        conn = get_master_db_conn()
        cursor = conn.cursor()
        
        # Check if email already exists
        cursor.execute("SELECT manager_id FROM manager_configs WHERE email = %s", (config.email,))
        if cursor.fetchone():
            raise HTTPException(status_code=400, detail="Email already registered")

        query = """
            INSERT INTO manager_configs 
            (full_name, email, password_hash, company_name, db_host, db_user, db_pass, db_name, db_port)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) 
            RETURNING manager_id;
        """
        cursor.execute(query, (
            config.full_name, config.email, hashed_pwd, config.company_name, 
            config.db_host, config.db_user, config.db_pass, config.db_name, config.db_port
        ))
        
        new_id = cursor.fetchone()[0]
        conn.commit()
        return {"status": "success", "manager_id": str(new_id)}
    
    except Exception as e:
        if conn: conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn: conn.close()

# --- UPDATED LOGIN ---
@app.post("/api/v1/login")
async def login(credentials: LoginSchema):
    conn = None
    try:
        conn = get_master_db_conn()
        cursor = conn.cursor()
        
        # Fetch user by email
        cursor.execute("SELECT manager_id, full_name, password_hash FROM manager_configs WHERE email = %s", (credentials.email,))
        user = cursor.fetchone()
        
        if not user:
            raise HTTPException(status_code=401, detail="Invalid email or password")
        
        manager_id, full_name, hashed_password = user
        
        # Verify Password
        if not verify_password(credentials.password, hashed_password):
            raise HTTPException(status_code=401, detail="Invalid email or password")
            
        return {
            "manager_id": str(manager_id), 
            "name": full_name,
            "status": "success"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Login Error: {str(e)}")
    finally:
        if conn: conn.close()
        
token_blacklist = set()

@app.post("/api/v1/logout")
async def logout_manager(authorization: str = Header(None)):
    if not authorization:
        raise HTTPException(status_code=400, detail="No token provided")
    
    # Extract token from "Bearer <token>"
    token = authorization.split(" ")[1]
    
    # Add to blacklist
    token_blacklist.add(token)
    
    return {"message": "Successfully logged out"}
# 1. Get Profile Details
@app.get("/api/v1/manager/profile/{manager_id}")
async def get_manager_profile(manager_id: str):
    conn = get_master_db_conn()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT full_name, email, company_name 
            FROM manager_configs 
            WHERE manager_id = %s
        """, (manager_id,))
        user = cursor.fetchone()
        if not user:
            raise HTTPException(status_code=404, detail="Manager not found")
            
        return {
            "full_name": user[0],
            "email": user[1],
            "company_name": user[2]
        }
    finally:
        conn.close()

# 2. Update Password
@app.post("/api/v1/manager/update-password")
async def update_password(data: PasswordUpdate):
    conn = get_master_db_conn()
    try:
        # Hash the new password using your existing hash_password function
        new_hashed_pwd = hash_password(data.new_password)
        
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE manager_configs 
            SET password_hash = %s 
            WHERE manager_id = %s
        """, (new_hashed_pwd, data.manager_id))
        
        conn.commit()
        return {"status": "success", "message": "Password updated successfully"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()
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