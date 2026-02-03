import sys
import psycopg2
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("EcomOps-Advanced-DB-Toolbox")

def get_conn(h, u, p, db, prt):
    return psycopg2.connect(host=h, user=u, password=p, dbname=db, port=prt, sslmode='require')


@mcp.tool()
def get_activity_summary(host, user, password, db_name, port, manager_id: str, timeframe: str = "daily"):
    """
    Manager Tool: Provides a real-time summary of database activities (transactions, 
    modifications, compliance) across the entire system for a specific manager.
    """
    conn = get_conn(host, user, password, db_name, port)
    try:
        with conn.cursor() as cursor:
            # We use the 'global_activity_feed' view which unions all your activity tables
            query = """
                SELECT 
                    activity_type, 
                    COUNT(*) as count,
                    COUNT(CASE WHEN compliance_check = FALSE THEN 1 END) as issues
                FROM global_activity_feed 
                WHERE manager_id = %s 
                AND created_at >= CURRENT_DATE
                GROUP BY activity_type;
            """
            
            cursor.execute(query, (manager_id,))
            rows = cursor.fetchall()
            
            if not rows:
                return f"No activity recorded today for Manager ID: {manager_id}"

            # Formatting as a dictionary for the LLM to process
            summary = {row[0]: {"total_events": row[1], "compliance_flags": row[2]} for row in rows}
            
            # We return a string representation or JSON so MCP can transmit it
            import json
            return json.dumps({
                "manager_id": manager_id,
                "timeframe": timeframe,
                "summary": summary,
                "total_actions_today": sum(item['total_events'] for item in summary.values())
            }, indent=2)

    except Exception as e:
        return f"Database Error: {str(e)}"
    finally:
        conn.close()
        
@mcp.tool()
def get_recent_activity(host, user, password, db_name, port,manager_id: str, limit: int = 10, category: str = None):
    """
    Incident Investigation Tool: Returns a detailed trace of recent system changes.
    Use this to audit trails, investigate unexpected changes, or track specific incidents.
    - category: Optional filter (e.g., 'TRANSACTION', 'INVENTORY', 'SECURITY')
    """
    conn = get_conn(host, user, password, db_name, port)
    try:
        with conn.cursor() as cursor:
            # Base query targeting the audit trace
            query = """
                SELECT timestamp, category, reference_id, action, details
                FROM global_audit_trace
                WHERE manager_id = %s
            """
            params = [manager_id]

            if category:
                query += " AND category = %s"
                params.append(category.upper())

            # Always order by newest first
            query += " ORDER BY timestamp DESC LIMIT %s;"
            params.append(limit)

            cursor.execute(query, tuple(params))
            rows = cursor.fetchall()

            if not rows:
                return f"No recent activity found for Manager {manager_id}."

            # Format the detailed trace for the AI
            trace_results = []
            for row in rows:
                trace_results.append({
                    "time": row[0].strftime("%Y-%m-%d %H:%M:%S"),
                    "type": row[1],
                    "ref": row[2],
                    "action": row[3],
                    "details": row[4]
                })

            import json
            return json.dumps({
                "manager_id": manager_id,
                "record_count": len(trace_results),
                "activities": trace_results
            }, indent=2)

    except Exception as e:
        return f"Audit Error: {str(e)}"
    finally:
        conn.close()
        
@mcp.tool()
def get_user_activity(host, user, password, db_name, port,manager_id: str, staff_identifier: str, days_back: int = 7):
    """
    Accountability Tool: Inspects the behavior of a specific employee or contractor.
    Use this for privileged user reviews or monitoring contractor activity.
    - staff_identifier: Can be the Staff Name or Staff ID.
    """
    conn = get_conn(host, user, password, db_name, port)
    try:
        with conn.cursor() as cursor:
            # Flexible search for Name or ID
            query = """
                SELECT action_timestamp, staff_name, staff_role, action_type, table_affected, details
                FROM staff_activity_trace
                WHERE manager_id = %s 
                AND (staff_name ILIKE %s OR staff_id = %s)
                AND action_timestamp >= CURRENT_DATE - INTERVAL '%s days'
                ORDER BY action_timestamp DESC;
            """
            search_val = f"%{staff_identifier}%"
            params = (manager_id, search_val, staff_identifier, days_back)

            cursor.execute(query, params)
            rows = cursor.fetchall()

            if not rows:
                return f"No activity records found for '{staff_identifier}' in the last {days_back} days."

            # Organize the behavior report
            report = []
            for row in rows:
                report.append({
                    "timestamp": row[0].strftime("%Y-%m-%d %H:%M:%S"),
                    "user": f"{row[1]} ({row[2]})",
                    "action": f"{row[3]} on {row[4]}",
                    "description": row[5]
                })

            import json
            return json.dumps({
                "target_user": staff_identifier,
                "summary": f"Found {len(report)} actions",
                "logs": report
            }, indent=2)

    except Exception as e:
        return f"Accountability Audit Error: {str(e)}"
    finally:
        conn.close()
        
@mcp.tool()
def get_data_modifications(host, user, password, db_name, port,manager_id: str, table_name: str = None, action_type: str = None, limit: int = 20):
    """
    Data Integrity Tool: Tracks specific INSERT, UPDATE, and DELETE operations.
    Use this for change approval audits, checking data integrity, or rollback investigations.
    - action_type: 'INSERT', 'UPDATE', or 'DELETE'
    """
    conn = get_conn(host, user, password, db_name, port)
    try:
        with conn.cursor() as cursor:
            # We query the system_audit_logs focusing on data state changes
            query = """
                SELECT action_timestamp, table_affected, action_type, old_data, new_data, staff_name
                FROM system_audit_logs
                WHERE manager_id = %s
            """
            params = [manager_id]

            if table_name:
                query += " AND table_affected = %s"
                params.append(table_name.lower())
            
            if action_type:
                query += " AND action_type = %s"
                params.append(action_type.upper())

            query += " ORDER BY action_timestamp DESC LIMIT %s;"
            params.append(limit)

            cursor.execute(query, tuple(params))
            rows = cursor.fetchall()

            if not rows:
                return "No matching data modifications found."

            results = []
            for row in rows:
                results.append({
                    "timestamp": row[0].strftime("%Y-%m-%d %H:%M:%S"),
                    "table": row[1],
                    "action": row[2],
                    "changed_by": row[5],
                    "before": row[3], # JSON of old values
                    "after": row[4]   # JSON of new values
                })

            import json
            return json.dumps(results, indent=2)

    except Exception as e:
        return f"Modification Audit Error: {str(e)}"
    finally:
        conn.close()
        
@mcp.tool()
def get_active_connections(host, user, password, db_name, port,manager_id: str):
    """
    Live Health Tool: Inspects current database load and connection status.
    Use this to detect capacity issues, connection leaks, or system slowness.
    """
    conn = get_conn(host, user, password, db_name, port)
    try:
        with conn.cursor() as cursor:
            # We query the internal postgres stats table
            # We filter by database name to only see YOUR data connections
            query = """
                SELECT 
                    pid, 
                    usename, 
                    state, 
                    query, 
                    backend_start, 
                    client_addr 
                FROM pg_stat_activity 
                WHERE datname = current_database()
                AND state IS NOT NULL;
            """
            
            cursor.execute(query)
            rows = cursor.fetchall()
            
            if not rows:
                return "No active connections detected (highly unusual)."

            connections = []
            states = {"active": 0, "idle": 0, "other": 0}

            for row in rows:
                state = row[2]
                if state == 'active': states['active'] += 1
                elif state == 'idle': states['idle'] += 1
                else: states['other'] += 1

                connections.append({
                    "process_id": row[0],
                    "user": row[1],
                    "status": state,
                    "current_query": row[3] if state == 'active' else "None (Idle)",
                    "connected_since": row[4].strftime("%H:%M:%S"),
                    "ip": row[5] or "Internal"
                })

            import json
            return json.dumps({
                "summary": states,
                "total_connections": len(connections),
                "details": connections
            }, indent=2)

    except Exception as e:
        return f"System Health Error: {str(e)}"
    finally:
        conn.close()
        
@mcp.tool()
def get_slow_queries(host, user, password, db_name, port,manager_id: str, limit: int = 5):
    """
    Performance Tool: Identifies database bottlenecks and slow-running queries.
    Use this for root-cause analysis of system slowness and SLA enforcement.
    """
    conn = get_conn(host, user, password, db_name, port)
    try:
        with conn.cursor() as cursor:
            # We look for queries that take the most cumulative time
            query = """
                SELECT 
                    query, 
                    calls, 
                    total_exec_time / 1000 as total_seconds, 
                    mean_exec_time as avg_ms,
                    rows
                FROM pg_stat_statements
                WHERE query NOT LIKE '%%pg_stat%%'  -- Exclude monitoring queries
                ORDER BY total_exec_time DESC
                LIMIT %s;
            """
            
            cursor.execute(query, (limit,))
            rows = cursor.fetchall()
            
            if not rows:
                return "No performance data available. (Ensure pg_stat_statements is enabled)"

            bottlenecks = []
            for row in rows:
                bottlenecks.append({
                    "query_snippet": row[0][:100] + "...", # Truncate for readability
                    "executions": row[1],
                    "total_time_spent_sec": round(row[2], 2),
                    "average_latency_ms": round(row[3], 2),
                    "rows_processed": row[4]
                })

            import json
            return json.dumps({
                "status": "Performance Report",
                "top_bottlenecks": bottlenecks
            }, indent=2)

    except Exception as e:
        return f"Performance Audit Error: {str(e)}"
    finally:
        conn.close()
    
@mcp.tool()
def get_failed_operations(host, user, password, db_name, port,manager_id: str, limit: int = 10):
    """
    Diagnostic Tool: Tracks and reviews database query failures and system errors.
    Use this for incident review, bug investigation, and troubleshooting failed actions.
    """
    conn = get_conn(host, user, password, db_name, port)
    try:
        with conn.cursor() as cursor:
            query = """
                SELECT timestamp, error_code, error_message, failed_query
                FROM system_error_logs
                WHERE manager_id = %s
                ORDER BY timestamp DESC
                LIMIT %s;
            """
            cursor.execute(query, (manager_id, limit))
            rows = cursor.fetchall()

            if not rows:
                return "Good news: No failed operations recorded recently."

            failures = []
            for row in rows:
                failures.append({
                    "time": row[0].strftime("%Y-%m-%d %H:%M:%S"),
                    "code": row[1],
                    "error": row[2],
                    "query_context": row[3] if row[3] else "N/A"
                })

            import json
            return json.dumps({
                "status": "Error Audit Results",
                "failures": failures
            }, indent=2)

    except Exception as e:
        return f"Diagnostic Tool Error: {str(e)}"
    finally:
        conn.close()
        
@mcp.tool()
def get_privileged_activity(host, user, password, db_name, port,manager_id: str, timeframe_days: int = 30):
    """
    Security Tool: Monitors admin-level access and sensitive system changes.
    Use this for insider threat detection, compliance reporting, and auditing high-level permissions.
    """
    conn = get_conn(host, user, password, db_name, port)
    try:
        # Use 'with' to define the cursor
        with conn.cursor() as cursor:
            # We look for high-impact actions like GRANT, REVOKE, ALTER, or access to sensitive tables
            query = """
                SELECT action_timestamp, staff_name, staff_role, action_type, details
                FROM staff_activity_trace
                WHERE manager_id = %s 
                AND (staff_role = 'Admin' OR action_type IN ('GRANT', 'REVOKE', 'ALTER', 'DROP'))
                AND action_timestamp >= CURRENT_DATE - INTERVAL '%s days'
                ORDER BY action_timestamp DESC;
            """
            cursor.execute(query, (manager_id, timeframe_days))
            rows = cursor.fetchall()

            if not rows:
                return f"No privileged activities detected in the last {timeframe_days} days."

            security_logs = []
            for row in rows:
                security_logs.append({
                    "timestamp": row[0].strftime("%Y-%m-%d %H:%M:%S"),
                    "administrator": row[1],
                    "action": row[3],
                    "impact": row[4]
                })

            import json
            return json.dumps({
                "audit_scope": f"{timeframe_days} Days",
                "alert_level": "High" if len(security_logs) > 5 else "Normal",
                "privileged_actions": security_logs
            }, indent=2)

    except Exception as e:
        return f"Security Audit Error: {str(e)}"
    finally:
        conn.close()

@mcp.tool()
def detect_anomalous_activity(host, user, password, db_name, port,manager_id: str):
    """
    Risk Detection Tool: Automatically flags unusual database behavior or potential threats.
    Use this for early-warning alerts and identifying automated risk patterns.
    """
    conn = get_conn(host, user, password, db_name, port)
    try:
        with conn.cursor() as cursor:
            # 1. Check for Volume Anomalies (Spikes in Deletes/Updates)
            cursor.execute("""
                WITH daily_stats AS (
                    SELECT count(*) as cnt 
                    FROM staff_activity_trace 
                    WHERE manager_id = %s AND action_timestamp > NOW() - INTERVAL '7 days'
                    GROUP BY date_trunc('day', action_timestamp)
                )
                SELECT AVG(cnt) FROM daily_stats;
            """, (manager_id,))
            avg_daily = cursor.fetchone()[0] or 0
            
            cursor.execute("""
                SELECT count(*), staff_name 
                FROM staff_activity_trace 
                WHERE manager_id = %s AND action_timestamp > NOW() - INTERVAL '1 hour'
                GROUP BY staff_name;
            """, (manager_id,))
            recent_stats = cursor.fetchall()

            anomalies = []
            for count, name in recent_stats:
                # If a user does 5x the daily average in 1 hour, flag it
                if count > (avg_daily * 0.5) and avg_daily > 0:
                    anomalies.append({
                        "type": "Volume Spike",
                        "severity": "High",
                        "details": f"User {name} performed {count} actions in 60 mins (Avg daily: {round(avg_daily)})"
                    })

            # 2. Check for Time-based Anomalies (Late night Admin work)
            cursor.execute("""
                SELECT staff_name, action_type, action_timestamp 
                FROM staff_activity_trace 
                WHERE manager_id = %s 
                AND EXTRACT(HOUR FROM action_timestamp) BETWEEN 0 AND 5
                AND action_timestamp > NOW() - INTERVAL '24 hours';
            """, (manager_id,))
            late_actions = cursor.fetchall()
            
            for name, action, ts in late_actions:
                anomalies.append({
                    "type": "Suspicious Timing",
                    "severity": "Medium",
                    "details": f"{name} performed {action} at {ts.strftime('%H:%M')} (After-hours)"
                })

            import json
            return json.dumps({
                "analysis_status": "Complete",
                "anomalies_found": len(anomalies),
                "alerts": anomalies
            }, indent=2)

    except Exception as e:
        return f"Anomaly Detection Error: {str(e)}"
    finally:
        conn.close()
        
@mcp.tool()
def get_growth_trends(host, user, password, db_name, port,manager_id: str):
    """
    Capacity Planning Tool: Analyzes table size growth and storage consumption.
    Use this for cost forecasting and predicting when database upgrades are needed.
    """
    conn = get_conn(host, user, password, db_name, port)
    try:
        with conn.cursor() as cursor:
            # This query calculates the size of all public tables in Megabytes
            query = """
                SELECT 
                    relname AS table_name,
                    round(pg_total_relation_size(relid) / 1024 / 1024, 2) AS size_mb,
                    reltuples AS row_count
                FROM pg_catalog.pg_statio_user_tables
                ORDER BY pg_total_relation_size(relid) DESC;
            """
            cursor.execute(query)
            rows = cursor.fetchall()
            
            if not rows:
                return "No table data found."

            table_metrics = []
            total_db_size = 0
            
            for row in rows:
                total_db_size += row[1]
                table_metrics.append({
                    "table": row[0],
                    "size_mb": row[1],
                    "approx_rows": int(row[2])
                })

            # Logic for the AI to interpret
            report = {
                "total_size_mb": round(total_db_size, 2),
                "storage_limit_utilization": f"{round((total_db_size / 500) * 100, 1)}%" if total_db_size < 500 else "Warning: Near Limit",
                "tables": table_metrics
            }

            import json
            return json.dumps(report, indent=2)

    except Exception as e:
        return f"Growth Analysis Error: {str(e)}"
    finally:
        conn.close()
        

@mcp.tool()
def inspect_schema(host, user, password, dbname, port):
    """Developer Tool: Lists all tables and their columns to help with DB development."""
    try:
        conn = get_conn(host, user, password, dbname, port)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name, column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = 'public';
            """)
            return str(cur.fetchall())
    except Exception as e: return f"Error: {str(e)}"
    finally: conn.close()

@mcp.tool()
def track_activity(host, user, password, dbname, port):
    """Manager Tool: Shows recent transactions, top selling products, or latest orders."""
    try:
        conn = get_conn(host, user, password, dbname, port)
        with conn.cursor() as cur:
            # Adjust this query to match your actual 'orders' or 'logs' table
            cur.execute("SELECT * FROM orders ORDER BY created_at DESC LIMIT 10;")
            return str(cur.fetchall())
    except Exception as e: return f"Error: {str(e)}"
    finally: conn.close()

@mcp.tool()
def execute_sql(host, user, password, dbname, port, sql_query):
    """General Tool: Executes raw SQL. Returns clear errors if names are wrong."""
    try:
        conn = get_conn(host, user, password, dbname, port)
        with conn.cursor() as cur:
            cur.execute(sql_query)
            if cur.description: 
                return str(cur.fetchall())
            conn.commit()
            return "Operation successful."
    except Exception as e:
        # This returns the error to the AI so it knows it guessed the wrong column!
        return f"SQL Error: {str(e)}. Check schema and try again."
    finally:
        if 'conn' in locals(): conn.close()
@mcp.tool()
def clone_product_by_name(host, user, password, dbname, port, source_name, new_name):
    """Developer Tool: Clones a product by omitting the ID to avoid unique constraints."""
    try:
        conn = get_conn(host, user, password, dbname, port)
        with conn.cursor() as cur:
            # 1. Get attributes from the source product
            cur.execute("SELECT price, stock FROM products WHERE name = %s LIMIT 1;", (source_name,))
            source = cur.fetchone()
            
            if not source:
                return f"Error: Source product '{source_name}' not found."
            
            price, stock = source

            # 2. Insert the new product with the same attributes
            cur.execute(
                "INSERT INTO products (name, price, stock) VALUES (%s, %s, %s) RETURNING id;",
                (new_name, price, stock)
            )
            new_id = cur.fetchone()[0]
            conn.commit()
            
            return f"Successfully added '{new_name}' (ID: {new_id}) with Price: {price} and Stock: {stock} (copied from '{source_name}')."
    except Exception as e:
        return f"Database Error: {str(e)}"
    finally:
        conn.close()
        
if __name__ == "__main__":
    mcp.run()