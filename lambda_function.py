import os
import praw
import psycopg2
from datetime import datetime

def lambda_handler(event, context):
    # Retrieve AWS Lambda environment variables
    db_url = os.environ['DATABASE_URL']
    client_id = os.environ['REDDIT_CLIENT_ID']
    client_secret = os.environ['REDDIT_CLIENT_SECRET']
    user_agent = "AWS_Lambda_Worker:v1.0"
    
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()
    
    try:
        # 1. Fetch all registered users and their refresh tokens
        cursor.execute("SELECT id, reddit_username, reddit_refresh_token FROM users;")
        users = cursor.fetchall()
        
        for user_id, username, refresh_token in users:
            # 2. Authenticate with Reddit as this specific user
            reddit = praw.Reddit(
                client_id=client_id,
                client_secret=client_secret,
                refresh_token=refresh_token,
                user_agent=user_agent
            )
            
            # 3. Fetch their latest 25 upvoted posts
            upvoted_posts = reddit.user.me().upvoted(limit=25)
            
            # 4. Insert into the interactions table, ignoring duplicates
            insert_query = """
                INSERT INTO reddit_interactions (
                    user_id, reddit_post_id, title, content, url, subreddit, interaction_timestamp
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id, reddit_post_id) DO NOTHING;
            """
            
            new_records = 0
            for post in upvoted_posts:
                post_content = getattr(post, 'selftext', '')
                cursor.execute(insert_query, (
                    user_id,
                    post.id,
                    post.title,
                    post_content,
                    f"https://reddit.com{post.permalink}",
                    post.subreddit.display_name,
                    datetime.now() # The exact time the worker discovered the upvote
                ))
                if cursor.rowcount > 0:
                    new_records += 1
            
            print(f"Processed user {username}: inserted {new_records} new upvotes.")
            
        conn.commit()
    except Exception as e:
        print(f"Pipeline error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

    return {"status": 200, "message": "Hourly ingestion complete."}