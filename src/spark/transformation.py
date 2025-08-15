import argparse
from os.path import join 
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

def get_tweets_data(df): #busca dados sobre tweets no arquivo json
    tweet_df = df.select(f.explode("data").alias("tweets"))\
                .select("tweets.author_id", "tweets.conversation_id",
                        "tweets.created_at", "tweets.id",
                        "tweets.public_metrics.*", "tweets.text")
    return tweet_df





def get_users_data(df): #busca dados sobre usuarios no arquivo json
    user_df = df.select(f.explode('includes.users').alias('users')).select('users.*')

    return user_df



def export_json(df, dest): #Salvando os arquivos json
    df.coalesce(1).write.mode("overwrite").json(dest)



def twitter_transformation(spark, src, dest, process_date):
    df = spark.read.json(src) # armanezando o df de acordo com o src

    tweet_df = get_tweets_data(df) # Definindo variaveis que contem o df dos tweets
    user_df = get_users_data(df) # Definindo variaveis que contem o df dos users

    table_dest = join(dest, '{table_name}',f'process_date={process_date}')# Join - juntando o diretorio(dest) com o nome da tabela e a data

    export_json(tweet_df, table_dest.format(table_name='tweet'))#vai salver os arquivos json na pasta tweet
    export_json(user_df, table_dest.format(table_name='user'))#vai salver os arquivos json na pasta user




if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Spark Twitter Transformation"
    )
    parser.add_argument("--src", required=True)
    parser.add_argument("--dest", required=True)
    parser.add_argument("--process-date", required=True)

    args = parser.parse_args()

    spark = SparkSession\
        .builder\
        .appName("twitter_transformation")\
        .getOrCreate()

    twitter_transformation(spark, args.src, args.dest, args.process_date)
