import boto3
from boto3.dynamodb.conditions import Key

def load_movie_table(resource=None):
    """ Load the Movie table with a few examples """
    if resource is None:
        resource = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = resource.Table('Movies')
    with table.batch_writer() as batch:
        batch.put_item(Item={
            'year': 1990,
            'title': 'Example 3'
        })
        batch.put_item(Item={
            'year': 2000,
            'title': 'Example 5'
        })

def update_movie_table(title, year, rating, plot, actors, resource=None):
    """ Update the Movies table with more information """
    if resource is None:
        resource = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    response = resource.Table('Movies').update_item(
        Key={
            'year': year,
            'title': title,
        },
        UpdateExpression='SET rating = :rating, plot = :plot, actors = :actors',
        ExpressionAttributeValues={
            ':rating': rating,
            ':plot': plot,
            ':actors': actors,
        },
        ReturnValues='UPDATED_NEW',
    )
    return response

def read_movie_table(resource=None):
    """ Scan the whole Movies table """
    if resource is None:
        resource = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = resource.Table('Movies')
    scan_kwargs = {
        'FilterExpression': Key('year').between(1990, 2000),
        'ProjectionExpression': "#yr, title, info.rating",
        'ExpressionAttributeNames': {"#yr": "year"},

    }

    done = False
    start_key = None
    while not done:
        if start_key:
            scan_kwargs['ExclusiveStartKey'] = start_key
        rsp = table.scan(**scan_kwargs)
        print(repr(rsp))
        start_key = rsp.get('LastEvaluatedKey', None)
        done  = start_key is None

def query_movie_table(year, resource=None):
    """ Scan the whole Movies table """
    if resource is None:
        resource = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = resource.Table('Movies')
    rsp = table.query(KeyConditionExpression=Key('year').eq(year))
    print(repr(rsp))

if __name__ == '__main__':
    print('Hello Dynamo DB')
    load_movie_table()

    rsp = update_movie_table('Example 5', 2000, '5', 'blah goes to away', 'Dustin Hoffman')
    print(repr(rsp))

    read_movie_table()
    query_movie_table(1990)



