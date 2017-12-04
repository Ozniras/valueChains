import psycopg2
from psycopg2 import sql
try:
    get_ipython().system('ln -s /var/run/postgresql/.s.PGSQL.5432 /tmp/.s.PGSQL.5432')
except:
    pass

conn = psycopg2.connect(database="datamyne20170728", user="ram22")
cur = conn.cursor()

def appendSql_bol(original, new):
    together = original + '_all'

    cur.execute("DROP TABLE IF EXISTS temp;")
    cur.execute(sql.SQL("""
    CREATE TABLE temp AS SELECT * 
    FROM {0} AS new
    WHERE NOT EXISTS 
    (SELECT 1 FROM {1} as orig  
    WHERE new.bill_id = orig.bill_id 
    AND new.container_number = orig.container_number);""").format(sql.Identifier(new), sql.Identifier(original)));
    cur.execute(sql.SQL("DROP TABLE IF EXISTS {0};").format(sql.Identifier(together)))
    cur.execute(sql.SQL("CREATE TABLE {0} AS SELECT * FROM {1};").format(sql.Identifier(together), sql.Identifier(original)))
    cur.execute(sql.SQL("INSERT INTO {0} (SELECT * FROM temp);").format(sql.Identifier(together)))
    cur.execute("DROP TABLE temp;")
    conn.commit()

def appendSql(original, new):
    together = original + '_all'

    cur.execute("DROP TABLE IF EXISTS temp;")
    cur.execute(sql.SQL("DROP TABLE IF EXISTS {0};").format(sql.Identifier(together)))
    
    cur.execute(sql.SQL("CREATE TABLE temp AS SELECT * FROM {0};").format(sql.Identifier(original)))
    cur.execute(sql.SQL("INSERT INTO temp (SELECT * FROM {0});").format(sql.Identifier(new)))
    cur.execute(sql.SQL("CREATE TABLE {0} AS SELECT DISTINCT * FROM temp;").format(sql.Identifier(together)))
    cur.execute("DROP TABLE temp;")
    conn.commit()
appendSql_bol('billoflading', 'billoflading_gsk')
appendSql('bol_pull_db_profiles', 'bol_pull_db_profiles_gsk')
appendSql('db_direct_companies', 'db_direct_companies_gsk')
conn.close()




