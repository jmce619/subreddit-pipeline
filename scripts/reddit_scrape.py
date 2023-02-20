
LOCAL_DIR='/home/airflow'
def main():
    SUBFIELDS = ['title', 'subreddit', 'created_utc', 'author', 'num_comments', 'score','url']
    # establish base url and stem
    BASE_URL = f"https://api.pushshift.io/reddit/search/submission" # also known as the "API endpoint" 
    stem = f"{BASE_URL}?subreddit=worldnews&size=500" # always pulling max of 500
    # instantiate empty list for temp storage
    posts = []
    # implement for loop with `time.sleep(2)`
    for i in range(1, 6):
        URL = "{}&after={}d".format(stem, 30 * i)
        response = requests.get(URL)
        assert response.status_code == 200
        mine = response.json()['data']
        df = pd.DataFrame.from_dict(mine)
        posts.append(df)
        time.sleep(2)
    # pd.concat storage list
    full = pd.concat(posts, sort=False)
    # if submission
    if kind == "submission":
        # select desired columns
        full = full[SUBFIELDS]
        # drop duplicates
        full.drop_duplicates(inplace = True)
    full['timestamp'] = full["created_utc"].map(dt.date.fromtimestamp)  


    full.to_csv(LOCAL_DIR + 'subreddit_data.csv')

if __name__ == '__main___':
    main()