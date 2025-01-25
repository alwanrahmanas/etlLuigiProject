import numpy as np

def handling_genres(df,colname):
    # Convert the list of genres into a string
        p = df[colname].apply(lambda x: ", ".join(x) if isinstance(x, list) else x)
        return p
def handling_anime(df):
    try: 
        episode_pattern = r"(\d+)\s*eps"
        duration_pattern = r"(\d+)\s*min"
        
        # Convert 'release_date' to datetime
        df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
        
        # Extract episodes and duration using regex
        df['episodes'] = df['episodes'].str.extract(episode_pattern).astype(float)
        df['duration'] = df['duration'].str.extract(duration_pattern).astype(float)
        
        # Convert 'episodes' column NaNs to "On-Going"
        df['episodes'] = df['episodes'].apply(lambda x: "On-Going" if pd.isna(x) else x)
        
        # Convert the list of genres into a string
        df['genres'] = handling_genres(df,'genres')
    except Exception as e:
        print(f"An error occurred while handling the dataframe: {e}")
        
    return df

def handling_manga(df):
    
     # Convert 'release_date' to datetime
        df['start_published'] = pd.to_datetime(df['start_published'], errors='coerce')
        df['end_published'] = pd.to_datetime(df['end_published'], errors='coerce')
        df['genres'] = handling_genres(df[['genres']],'genres')
        df['themes'] = handling_genres(df[['themes']],'themes')

        return df
        
        