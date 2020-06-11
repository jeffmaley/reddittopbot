import praw, json, boto3, datetime, time

praw_config_file = 'praw.json'

def get_config(praw_config_file):
    f = open(praw_config_file, 'r')
    config = json.loads(f.read())
    return config

def get_posts(config):
    posts = {}
    reddit = praw.Reddit(client_id=config['client_id'], client_secret=config['client_secret'], user_agent=config['user_agent'])
    subreddit = reddit.subreddit("askreddit")
    for submission in subreddit.top(time_filter="year",limit=50):
        post = reddit.submission(id=submission.id)
        print(post.title)
        print(post.created_utc)
        print(post.author)
        post_data = {'id': submission.id, 'title': post.title, 'created': post.created_utc, 'author': post.author}
        posts[submission.id] = post_data
    return posts

def push_to_ddb(posts):
    cur_date_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
    client = boto3.client('dynamodb')
    for key in posts.keys():
        response = client.put_item(TableName='reddittopbot-askreddit', Item={
            'date': {'N': cur_date_time},
            'id': {'S': posts[key]['id']},
            'title': {'S': posts[key]['title']},
            'created': {'S': str(time.asctime(time.gmtime(posts[key]['created'])))},
            'author': {'S': str(posts[key]['author'])}
        })
        print(response)
    return 0

def main():
    config = get_config(praw_config_file)
    posts = get_posts(config)
    push_to_ddb(posts)
    return 0

if __name__ == "__main__":
    main()