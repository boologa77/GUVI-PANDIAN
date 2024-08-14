import streamlit as st
import boto3
import json

# Initialize Boto3 client with explicit credentials
client = boto3.client(
    'lambda',
    region_name='ap-southeast-2',
    aws_access_key_id='A',
    aws_secret_access_key=''
)

# Define the ARN of your Lambda function
LAMBDA_ARN = '"

st.title("Trending Hashtags")

# Post Composition Section
post_content = st.text_area("Write your post here:")
if st.button("Post"):
    response = client.invoke(
        FunctionName=LAMBDA_ARN,
        InvocationType='RequestResponse',
        Payload=json.dumps({"routeKey": "POST /submitPost", "body": {"content": post_content}}),
    )
    result = json.loads(response['Payload'].read())
    if response['StatusCode'] == 200:
        st.success("Post submitted successfully!")
    else:
        st.error("Failed to submit post.")

# Trending Hashtags Section
if st.button("Show Trending Hashtags"):
    response = client.invoke(
        FunctionName=LAMBDA_ARN,
        InvocationType='RequestResponse',
        Payload=json.dumps({"routeKey": "GET /trendingHashtags"})
    )
    result = json.loads(response['Payload'].read())

    if response['StatusCode'] == 200:
        # Parse the JSON string in the 'body' to extract hashtags
        body = json.loads(result['body'])
        trending_hashtags = body.get("hashtags", [])
        if trending_hashtags:
            st.write("Trending Hashtags:")
            for hashtag in trending_hashtags:
                st.write(f"#{hashtag}")
        else:
            st.write("No trending hashtags found.")
    else:
        st.error("Failed to fetch trending hashtags.")
