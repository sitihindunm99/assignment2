import boto3
import time
s3 = boto3.resource("s3")

# Query string to execute
track_query = "SELECT track_name, SUM(streams) AS total_streams \nFROM \"AwsDataCatalog\".\"assignment2-db\".\"cleaned\" \nWHERE pivot = 0 \nGROUP BY track_name \nORDER BY total_streams DESC \nLIMIT 10;"
genre_query = "SELECT country, artist_genre, SUM(streams) AS total_streams \nFROM \"AwsDataCatalog\".\"assignment2-db\".\"cleaned\" \nWHERE country <> 'Global' \nGROUP BY country, artist_genre \nHAVING COUNT(*) = ( \n    SELECT MAX(num_tracks) \n    FROM ( \n        SELECT country, artist_genre, COUNT(*) AS num_tracks \n        FROM \"AwsDataCatalog\".\"assignment2-db\".\"cleaned\" \n        WHERE country <> 'Global' \n        GROUP BY country, artist_genre \n    ) t2 \n    WHERE t2.country = cleaned.country \n) \nORDER BY country;"
artist_query = "SELECT artist_individual, COUNT(DISTINCT uri) AS num_tracks \nFROM \"AwsDataCatalog\".\"assignment2-db\".\"cleaned\" \nGROUP BY artist_individual \nORDER BY num_tracks DESC \nLIMIT 10;"
language_query = "SELECT language, track_name, SUM(streams) AS total_streams \nFROM  \"AwsDataCatalog\".\"assignment2-db\".\"cleaned\" \nWHERE language IS NOT NULL AND language <> 'Global' \nGROUP BY language, track_name \nORDER BY total_streams DESC;"
tempo_query = "SELECT artist_genre, tempo \nFROM cleaned \nWHERE artist_genre IN ( \n    SELECT artist_genre \n    FROM cleaned \n    WHERE artist_genre != '0' \n    GROUP BY artist_genre \n    ORDER BY SUM(streams) DESC \n    LIMIT 5 \n  );"

queries = {"track_query":track_query, "genre_query":genre_query, "artist_query":artist_query, "language_query":language_query, "tempo_query": tempo_query}

def lambda_handler(event, context):
    # Start the query execution
    for filename, actual_query in queries.items():
        run_athena_query(actual_query, filename)

def run_athena_query(actual_query, filename):
    
    client = boto3.client('athena')
    DATABASE = 'assignment2-db'
    output='s3://is459-hindun-assignment2/query-result/'
       
    queryStart = client.start_query_execution(
        QueryString=actual_query,
        QueryExecutionContext={
            'Database': DATABASE
        },
        ResultConfiguration={
            'OutputLocation': output
        }
    )
   
    queryId = queryStart['QueryExecutionId']
    time.sleep(3)
    queryLoc = "is459-hindun-assignment2/query-result/" + queryId + ".csv"

    s3.Object("is459-hindun-assignment2", "query-result/" + filename + ".csv").copy_from(CopySource = queryLoc)

    response = s3.Object('is459-hindun-assignment2','query-result/'+queryId+".csv").delete()
    response = s3.Object('is459-hindun-assignment2','query-result/'+queryId+".csv.metadata").delete()

    return response