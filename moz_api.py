## This script calls the Moz API and pulls the Domain Authority and Page Authority
## of individual URLs. It then uploads the DA and PA values into the corresponding
## SQL table.

from mozscape import Mozscape
from mysqldb import db
from contextlib import closing
import traceback
import time

# Imput your member ID and secret key here
client = Mozscape('[memberID]', '[secretKey]')

# Select individual URLs from your SQL table
with closing(db.cursor()) as cur:
    cur.execute("""
    select URL from db.table1
    WHERE (Domain_Authority = '' OR Domain_Authority IS NULL)
    """)
    rows = cur.fetchall()
    for record in rows:
        time.sleep(10)
        url = record[0]
        try:
            domainauth = client.urlMetrics([url], Mozscape.UMCols.domainAuthority)
            clean_domainauth = domainauth[0]['pda']

            pageauth = client.urlMetrics([url], Mozscape.UMCols.pageAuthority)
            clean_pageauth = pageauth[0]['upa']

            cur.execute("""
            UPDATE db.table1 SET Domain_Authority = %s, Page_Authority = %s                                                                                          WHERE URL = %s
            """, (clean_domainauth, clean_pageauth, url))
            db.commit()

        except Exception as e:
            #traceback.print_exc()
            print "got error with " + url
