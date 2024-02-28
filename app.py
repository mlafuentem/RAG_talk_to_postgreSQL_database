import os
from openai import OpenAI
import json
import os
import asyncio
import asyncpg
from dotenv import load_dotenv

#We create a .env file in the same directory as the script. We store our secret API keys, URLs and passwords there
load_dotenv(<'PATH TO YOUR .ENV FILE'>)

# We create a query that will ask the database for the schema of the 'auth' and 'public' schemas (this is just an example).
# We get the table schema, table catalogue, table name, and type of each field. 
query= '''SELECT table_schema AS schema_name,
       table_catalog AS catalog_name,
       table_name,
       table_type AS type,
       pg_size_pretty(pg_total_relation_size('"' || table_schema || '"."' || table_name || '"'))::TEXT AS total_bytes,
       array_to_json(array_agg(row_to_json(t))) AS columns
FROM information_schema.tables tb
         INNER JOIN LATERAL ( SELECT column_name, data_type FROM information_schema.columns WHERE table_schema IN ('auth', 'public') AND table_name = tb.table_name ) t ON true
GROUP BY table_schema, table_catalog, table_name, table_type;'''

#We define a function that will run any raw SQL query against a PostgreSQL database.
async def ask_database(query: str):
    DATABASE_URL = os.environ.get("SUPABASE_URI")  # Your Supabase Database URL
    DATABASE_PASSWORD = os.environ.get("SUPABASE_PASSWORD")  # Your Supabase Database Password

    # Assuming your DATABASE_URL is in the format:
    # postgres://{user}:{password}@{hostname}:{port}/{database_name}
    # And you're replacing {password} with the actual password.
    conn_details = DATABASE_URL.replace('{password}', DATABASE_PASSWORD)

        # Initialize conn as None
    conn = None

    try:
        # Connect to the database
        conn = await asyncpg.connect(conn_details)
        # Execute raw SQL query
        data = await conn.fetch(query)
        return data
    except Exception as e:
        return f"Error: {str(e)}"
    finally:
        if conn:
            await conn.close()

#We get the schema of the 'auth' and 'public' schemas as explained above.
database_schema= asyncio.run(ask_database(query))

#We define a function call that will be used by our chat completion call on OpenAI ChatGPT.
#This function will transform a user text question into a postgresql query.
tools= [{
    "type": "function",
    "function": {
        "name": "ask_database",
        "description": "Use this function to answer user questions about an app database. The user will input a question using simple text and you will answer it producing a SQL query using postgresql guidelines",
        "parameters": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": f"""
                    This function call will answer users' questions. 
                    This will write a postgreSQL query to extract info to answer the user's question. 
                    The query will be written according to this specific schema: {database_schema}.
                    DON'T MAKE UP COLUMN NAMES AND CHECK TWICE WITH THE DATABASE SCHEMA BEFORE OUTPUTTING A QUERY.
                    """,
      }
    },
    "required": ["query"],
  },
}}]

#We get the OpenAI API key from the .env file.
client= OpenAI(
  api_key=os.environ.get("OPENAI_API_KEY"),
)

#We create a chat completion call on OpenAI ChatGPT and we ask anything we want to know in the "user" content.
chat_completion = client.chat.completions.create(
  messages=[
    {
        "role": "system",
        "content": f"You are a helpful assistant. You answer user questions by generating postgresql queries (raw SQL queries). The query will be written according to this specific schema: {database_schema}. DON'T MAKE UP COLUMN NAMES AND CHECK TWICE WITH THE DATABASE SCHEMA BEFORE OUTPUTTING A QUERY."
    }, 
    {
        "role": "user",
        "content": "give me all the users"
    }
  ], 
  model="gpt-3.5-turbo",
  tools=tools
)

#If the chat completion call is successful and uses the ask_database function, then we get the result of the query.
tool_call= chat_completion.choices[0].message.tool_calls[0]

if tool_call.function.name == "ask_database":
  query = json.loads(tool_call.function.arguments)["query"]
  result = asyncio.run(ask_database(query))
  print('QUERY:', query, '\n', 'RESULT:', result)
