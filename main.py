import sqlalchemy
import os
import logging
from flask import jsonify

# Environment variables are defined within the Cloud Function configuration
# hosted by Google Cloud Platform.
db_user = os.environ.get("DB_USER")
db_pass = os.environ.get("DB_PASS")
db_name = os.environ.get("DB_NAME")
cloud_sql_connection_name = os.environ.get("CLOUD_SQL_CONNECTION_NAME")

log = logging.getLogger()

# This small script defines what happens when the corresponding URL
# (based on the function name below) is hit. However, the most important
# thing to understand is how Cloud Function's serverless model works:
#
# - There is no fixed infrastructure. Instead, depending on demand,
#   Cloud Functions will create one or more instances of this script.
# - EACH instance of the script may serve many API requests. So you might
#   have 3 instances of the script, each of which serves 1,000
#   requests.
# - The way that works is, the entire script is only run once per instance.
#   Then after that, only the specific function below (get_drug_targets) is run.
#
# That allows for more efficient behavior. For example the database connection
# pool that we create first is only created 1x per instance, since it isn't
# part of the function itself. If a script instance serves 1,000
# requests, it still might only use a single database connection to do so
# (if the connection gets dropped, the pool can get a new one).
#
# Behind the scenes, Cloud Functions is using Flask to serve the function
# and tie it to the URL.
#
# I didn't add authentication here, but a production system would require
# an authorization header with a token.


# Connect to the Postgres database where the target data is stored,
# establishing a connection pool that can be used across requests.
db = sqlalchemy.create_engine(
    sqlalchemy.engine.url.URL(
        drivername="postgresql+psycopg2",
        username=db_user,
        password=db_pass,
        database=db_name,
        query={"host": "/cloudsql/{}".format(cloud_sql_connection_name)},
    ),
    # By default use a pool size of 1, since each Cloud Function
    # instance will run in serial (there will be potentially
    # many instances but they live separately). Allow a temporary
    # overflow if something goes wrong.
    pool_size=1,
    max_overflow=2,
    # Fail and throw an error if we can't get a DB connection
    # in this number of seconds
    pool_timeout=30
)


def get_drug_targets(request):
    '''
    Accepts drugbank_id as a query parameter and returns JSON containing
    a list of drug targets.

    Cloud Functions currently lacks the ability to customize URLs
    so unlike with AWS Lambda which is the equivalent on AWS,
    we can't use this to create a RESTful API, i.e.
    GET /get_drug_targets/[drugbank_id]

    I'm guessing they'll add the ability at some point.
    '''

    drugbank_id = request.args['drugbank_id']

    # Use a prepared statement to prevent SQL injection attacks
    #
    # If the size of the two tables becomes quite large, the below
    # may be more efficient than a JOIN.
    sql = '''
        SELECT
            drugbank_target
        FROM
            drugbank_targets
        WHERE
            drugbank_drug = (SELECT id FROM drugbank_drugs WHERE 
                             drugbank_provided_id = :drugbank_id);
          '''
    stmt = sqlalchemy.text(sql)
    targets = []

    with db.connect() as conn:
        result = conn.execute(stmt, drugbank_id=drugbank_id)
        for row in result:
            targets.append(row['drugbank_target'])

    # We could just return the array as the root element,
    # so the JSON doc would be just ['target1', 'target2'].
    # However, it's best to always use a dict with a key
    # so that additional keys can be added as needed in the
    # future (for example, containing some metadata about the
    # query) without breaking clients' processing patterns.
    #
    # For example it's common to cap results at 100 records
    # for performance reasons, so 500 records require 5 requests.
    # In that case the metadata might indicate the number of pages
    # and the current page.
    ans = {
        'targets': targets
    }

    return jsonify(ans)
