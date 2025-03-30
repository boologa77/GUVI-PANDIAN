import json
import boto3
import uuid
from collections import Counter

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb')
posts_table = dynamodb.Table('Posts')
trending_table = dynamodb.Table('TrendingHashtags')

def lambda_handler(event, context):
    # Ensure the event contains a routeKey
    if 'routeKey' in event:
        route = event['routeKey']
        
        # Route to the appropriate function based on the routeKey
        if route == 'POST /submitPost':
            return handle_post_submission(event)
        elif route == 'GET /trendingHashtags':
            return handle_trending_hashtags(event)
    
    # If routeKey is missing or invalid, return an error
    return {
        'statusCode': 400,
        'body': json.dumps('Invalid request')
    }

def handle_post_submission(event):
    # Extract content from the event body
    post_content = event['body']['content']
    hashtags = extract_hashtags(post_content)
    
    # Save the post and hashtags to the DynamoDB table
    posts_table.put_item(
        Item={
            'post_id': str(uuid.uuid4()),
            'content': post_content,
            'hashtags': hashtags
        }
    )
    
    # Return success response
    return {
        'statusCode': 200,
        'body': json.dumps('Post stored successfully!')
    }

def handle_trending_hashtags(event):
    # Scan the DynamoDB table to retrieve all posts
    response = posts_table.scan()
    items = response['Items']
    
    # Aggregate hashtags and count occurrences
    hashtags = []
    for item in items:
        hashtags.extend(item['hashtags'])
    
    # Identify top 10 trending hashtags
    trending_hashtags = Counter(hashtags).most_common(10)
    trending_hashtags = [hashtag for hashtag, count in trending_hashtags]
    
    # Return the trending hashtags
    return {
        'statusCode': 200,
        'body': json.dumps({'hashtags': trending_hashtags})
    }

def extract_hashtags(post_content):
    # Extract hashtags from the post content
    return [word for word in post_content.split() if word.startswith("#")]
