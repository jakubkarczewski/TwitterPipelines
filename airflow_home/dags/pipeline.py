from os.path import join
from pprint import pprint

import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

from twitter_extractor import TwitterExtractor

args = {
    'owner': 'jakubkarczewski',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='simple_twitter_pipeline',
    default_args=args,
    schedule_interval=None,
)


# [START howto_operator_python]
def print_context(ds, **kwargs):
    pprint(kwargs)
    print(ds)
    return 'Sample logs.'


run_this = PythonOperator(
    task_id='print_the_context',
    provide_context=True,
    python_callable=print_context,
    dag=dag,
)
# [END howto_operator_python]


# [START howto_operator_python_kwargs]
def get_tweets(keyword):
    """This is a function that will run within the DAG execution"""
    base_path = '/home/kuba/Development/TwitterPipelines'
    te = TwitterExtractor(access_tokens_path=join(base_path,
                                                  '.access_tokens.pkl'),
                          api_keys_path=join(base_path, '.api_keys.pkl'),
                          dump_path=join(base_path, 'data'))
    api = te.get_api_handle()
    tweets = te.get_tweets(api, keyword)
    df = te.to_pandas(tweets)
    df.to_csv(join(te.dump_path, f'{keyword}_tweets.csv'))
    return df


# Generate 5 sleeping tasks, sleeping from 0.0 to 0.4 seconds respectively
for keyword in ('dog', 'cat', 'trump'):
    task = PythonOperator(
        task_id=f'get_tweets_for_{keyword}',
        python_callable=get_tweets,
        op_kwargs={'keyword': keyword},
        dag=dag,
    )

    run_this >> task
# [END howto_operator_python_kwargs]