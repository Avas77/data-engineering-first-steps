import connect

def transform_avg_rating(movies, users):
    avg_rating = users.groupBy("movie_id").mean("rating").orderBy("movie_id")
    df = movies.join(avg_rating, movies.id == avg_rating.movie_id)
    df.drop("movie_id")
    return df

def load_df_to_db(df):
    mode = "overwrite"
    url = connect.db_url
    property = connect.db_properties
    df.write.jdbc(url=url, table="avg_rating", mode=mode, properties=property)

if __name__ == "__main__":
    movies_df = connect.connect_postgres("movies")
    users_df = connect.connect_postgres("users")
    df = transform_avg_rating(movies_df, users_df)
    load_df_to_db(df)
    

