import psycopg2
import mysql.connector
import time
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "local": {
        "hostname": os.getenv("LOCAL_HOSTNAME"),
        "user": os.getenv("LOCAL_USER"),
        "password": os.getenv("LOCAL_PASSWORD"),
        "port": int(os.getenv("LOCAL_PORT")),
        "database": os.getenv("LOCAL_DATABASE")
    },
    "neon": {
        "hostname": os.getenv("NEON_HOSTNAME"),
        "user": os.getenv("NEON_USER"),
        "password": os.getenv("NEON_PASSWORD"),
        "port": int(os.getenv("NEON_PORT")),
        "database": os.getenv("NEON_DATABASE")
    },
    "supabase": {
        "hostname": os.getenv("SUPABASE_HOSTNAME"),
        "user": os.getenv("SUPABASE_USER"),
        "password": os.getenv("SUPABASE_PASSWORD"),
        "port": int(os.getenv("SUPABASE_PORT")),
        "database": os.getenv("SUPABASE_DATABASE")
    },
    "aiven": {
        "hostname": os.getenv("AIVEN_HOSTNAME"),
        "user": os.getenv("AIVEN_USER"),
        "password": os.getenv("AIVEN_PASSWORD"),
        "port": int(os.getenv("AIVEN_PORT")),
        "database": os.getenv("AIVEN_DATABASE")
    }
}

NEON_SHARD_CATEGORIES = ['laptop', 'computer', 'desktop', 'all in one']
SUPABASE_SHARD_CATEGORIES = ['phone', 'tablet', 'smart band', 'smart watch', 'smart ring']

def connect_db(db_type):
    config = DB_CONFIG[db_type]
    try:
        if db_type in ["local", "neon", "supabase"]:
            conn = psycopg2.connect(
                host=config["hostname"],
                user=config["user"],
                password=config["password"],
                port=config["port"],
                database=config["database"]
            )
            print(f"{db_type.capitalize()}: Connected")
            return conn
        elif db_type == "aiven":
            conn = mysql.connector.connect(
                host=config["hostname"],
                user=config["user"],
                password=config["password"],
                port=config["port"],
                database=config["database"]
            )
            print(f"{db_type.capitalize()}: Connected")
            return conn
    except Exception as e:
        print(f"{db_type.capitalize()}: Failed ({e})")
        return None

def initialize_tables(db_conn, db_type):
    cursor = db_conn.cursor()
    try:
        if db_type in ["local", "neon", "supabase"]:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS product (
                    id SERIAL PRIMARY KEY,
                    category VARCHAR(255) NOT NULL,
                    brand VARCHAR(255),
                    model VARCHAR(255)
                );
            """)
        elif db_type == "aiven":
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS product (
                    id INT PRIMARY KEY,
                    category VARCHAR(255) NOT NULL,
                    brand VARCHAR(255),
                    model VARCHAR(255)
                );
            """)
        db_conn.commit()
        print(f"{db_type.capitalize()}: Ready")
    except Exception as e:
        print(f"{db_type.capitalize()}: Failed ({e})")
    finally:
        cursor.close()

def replicate_product_to_aiven(conn_aiven, id, category, brand, model, operation_type):
    if not conn_aiven:
        print(f"Aiven connection not ready for {operation_type} product ID {id}.")
        return

    cursor = conn_aiven.cursor()
    try:
        if operation_type == "INSERT":
            cursor.execute(
                "REPLACE INTO product (id, category, brand, model) VALUES (%s, %s, %s, %s);",
                (id, category, brand, model)
            )
            print(f"Aiven replicated insert {id}, {model}")
        elif operation_type == "UPDATE":
            cursor.execute(
                "UPDATE product SET category = %s, brand = %s, model = %s WHERE id = %s;",
                (category, brand, model, id)
            )
            print(f"Aiven replicated update {id}, {model}")
        elif operation_type == "DELETE":
            cursor.execute(
                "DELETE FROM product WHERE id = %s;",
                (id,)
            )
            print(f"Aiven replicated delete {id}, {model}")
        conn_aiven.commit()
    except mysql.connector.Error as err:
        print(f"Failed to replicate product id. {id}, {model} to Aiven: {err}")
        conn_aiven.rollback()
    except Exception as e:
        print(f"Unexpected error replicating product id. {id}, {model} to Aiven: {e}")
        conn_aiven.rollback()
    finally:
        cursor.close()

def shard_product_to_neon_and_supabase(conn_neon, conn_supabase, id, category, brand, model, operation_type):
    target_db_name = None
    target_conn = None

    if category is None:
        print(f"Category not available for sharding product ID {id}, {model} (operation: {operation_type}). Skip.")
        return

    if category.lower() in NEON_SHARD_CATEGORIES:
        target_db_name = "Neon"
        target_conn = conn_neon
    elif category.lower() in SUPABASE_SHARD_CATEGORIES:
        target_db_name = "Supabase"
        target_conn = conn_supabase
    else:
        print(f"Category '{category}' does not match for sharding. Skip product ID {id}.")
        return

    if not target_conn:
        print(f"Connection to {target_db_name} not ready for product ID {id}.")
        return

    cursor = target_conn.cursor()
    try:
        if operation_type == "INSERT":
            cursor.execute(
                "INSERT INTO product (id, category, brand, model) VALUES (%s, %s, %s, %s) ON CONFLICT (id) DO UPDATE SET category = EXCLUDED.category, brand = EXCLUDED.brand, model = EXCLUDED.model;",
                (id, category, brand, model)
            )
            print(f"{target_db_name} sharding insert {id}, {model}")
        elif operation_type == "UPDATE":
            cursor.execute(
                "UPDATE product SET category = %s, brand = %s, model = %s WHERE id = %s;",
                (category, brand, model, id)
            )
            print(f"{target_db_name} sharding update {id}, {model}")
        elif operation_type == "DELETE":
            cursor.execute(
                "DELETE FROM product WHERE id = %s;",
                (id,)
            )
            print(f"{target_db_name} sharding delete {id}, {model}")
        target_conn.commit()
    except psycopg2.Error as err:
        print(f"Failed to shard product id. {id}, {model} to {target_db_name}: {err}")
        target_conn.rollback()
    except Exception as e:
        print(f"Unexpected error sharding product id. {id}, {model} to {target_db_name}: {e}")
        target_conn.rollback()
    finally:
        cursor.close()

def monitor_local_db():
    print("Database\n")

    conn_local = connect_db("local")
    conn_neon = connect_db("neon")
    conn_supabase = connect_db("supabase")
    conn_aiven = connect_db("aiven")

    if not all([conn_local, conn_neon, conn_supabase, conn_aiven]):
        print("\nFailed to connect to all databases. Exiting.")
        return

    print("\nTable\n")

    initialize_tables(conn_local, "local")
    initialize_tables(conn_neon, "neon")
    initialize_tables(conn_supabase, "supabase")
    initialize_tables(conn_aiven, "aiven")

    last_known_products = {}

    try:
        cursor_local_init = conn_local.cursor()
        cursor_local_init.execute("SELECT id, category, brand, model FROM product;")
        for row in cursor_local_init.fetchall():
            last_known_products[row[0]] = (row[1], row[2], row[3])
        cursor_local_init.close()
        print("\nInitial product state loaded from Local Database")
    except Exception as e:
        print(f"\nFailed to load initial state from Local DB: {e}")

    print("Starting local database monitor:")

    while True:
        try:
            cursor_local = conn_local.cursor()
            cursor_local.execute("SELECT id, category, brand, model FROM product;")
            current_products = {row[0]: (row[1], row[2], row[3]) for row in cursor_local.fetchall()}
            cursor_local.close()

            for id, current_data in current_products.items():
                category, brand, model = current_data
                if id not in last_known_products:
                    print(f"\nLocal detected insert: {id}, {model}")
                    replicate_product_to_aiven(conn_aiven, id, category, brand, model, "INSERT")
                    shard_product_to_neon_and_supabase(conn_neon, conn_supabase, id, category, brand, model, "INSERT")
                elif last_known_products[id] != current_data:
                    print(f"\nLocal detected update: {id}, {model}")
                    replicate_product_to_aiven(conn_aiven, id, category, brand, model, "UPDATE")
                    shard_product_to_neon_and_supabase(conn_neon, conn_supabase, id, category, brand, model, "UPDATE")

            products_to_delete = [pid for pid in last_known_products if pid not in current_products]
            for id in products_to_delete:
                category_for_delete = last_known_products[id][0]
                model_for_delete = last_known_products[id][2]
                print(f"\nLocal detected delete: {id}, {model_for_delete}")
                replicate_product_to_aiven(conn_aiven, id, None, None, None, "DELETE")
                shard_product_to_neon_and_supabase(conn_neon, conn_supabase, id, category_for_delete, None, None, "DELETE")

            last_known_products = current_products.copy()

            time.sleep(5)
        except KeyboardInterrupt:
            print("\nMonitor stopped.")
            break
        except Exception as e:
            print(f"Error monitoring local DB: {e}")
            time.sleep(10)

    if conn_local: conn_local.close()
    if conn_neon: conn_neon.close()
    if conn_supabase: conn_supabase.close()
    if conn_aiven: conn_aiven.close()
    print("All database connections closed.")

if __name__ == "__main__":
    monitor_local_db()