import pandas as pd
import psycopg2
from psycopg2 import sql
try:
    get_ipython().system('ln -s /var/run/postgresql/.s.PGSQL.5432 /tmp/.s.PGSQL.5432')
except:
    pass

conn = psycopg2.connect(database="datamyne20170728", user="ram22")
cur = conn.cursor()

def createSql(incoming = '', out = '', prefix=''):

    infile = open(incoming, 'r')
    varNames = infile.readline()
    infile.close()

    # get rid of \n at end of line and text delimeter, then create list
    varNames = varNames[:-2].replace('"', '').split(',')
    varNames = [prefix + i for i in varNames]

    cur.execute(sql.SQL("DROP TABLE IF EXISTS {0};").format(sql.Identifier(out)))

    i = 0
    for field in varNames:
        if i == 0:
            cur.execute(sql.SQL("CREATE TABLE {0} ({1} varchar);").format(sql.Identifier(out), sql.Identifier(field)))
            i += 1
        else:
            cur.execute(sql.SQL("ALTER TABLE {0} ADD {1} varchar;").format(sql.Identifier(out), sql.Identifier(field)))

    infile = open(incoming, 'r') 
    cur.copy_expert(sql.SQL("COPY {0} FROM STDIN WITH CSV HEADER").format(sql.Identifier(out)), infile)
    infile.close()
    
    conn.commit()


nameFile = '../Datamyne_Delivery_7_28_2017/Data/meshintel-datamyne_bol_data.csv'
outSQL = 'billoflading'
createSql(nameFile, outSQL)

nameFile = '../20170803AdditionalDelivery/meshintel-datamyne_bol_data_GSK.csv'
outSQL = "billoflading_gsk"
createSql(nameFile, outSQL)

nameFile = '../Datamyne_Delivery_7_28_2017/Data/meshintel-datamyne_d_b_family_tree.csv'
outSQL = 'family_tree'
createSql(nameFile, outSQL)

nameFile = '../Datamyne_Delivery_7_28_2017/Data/meshintel-datamyne_bol_d_b_profiles.csv'
outSQL = 'bol_pull_db_profiles'
prefix = 'consignee_d_b_'
createSql(nameFile, outSQL, prefix)

nameFile = '../Datamyne_Delivery_7_28_2017/Data/meshintel-datamyne_d_b_all_companies.csv'
outSQL = 'db_direct_companies'
createSql(nameFile, outSQL)

nameFile = '../20170803AdditionalDelivery/meshintel-datamyne_bol_d_b_profiles_GSK.csv'
outSQL = 'bol_pull_db_profiles_gsk'
prefix = 'consignee_d_b_'
createSql(nameFile, outSQL, prefix)

nameFile = '../20170803AdditionalDelivery/meshintel-datamyne_d_b_all_companies_GSK.csv'
outSQL = 'db_direct_companies_gsk'
createSql(nameFile, outSQL)



conn.close()

