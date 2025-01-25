import luigi
from extract import scrape_anime_metadata


class ExtractAnimeData(luigi.Task):

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget("Raw/extract_anime_data.csv")

    def run(self):
        anime_metadata = scrape_anime_metadata()
        # After all data is collected, save to CSV
        df = pd.DataFrame(anime_metadata)
        df.to_csv(self.output().path, index=False)

class TransformAnimeData(luigi.Task):

    def requires(self):
        return ExtractAnimeData()

    def output(self):
        return luigi.LocalTarget("transform/transform_anime_data.csv")

    def run(self):
        df = pd.read_csv(self.input().path)
        df = handling_df(df)
        df.to_csv(self.output().path, index=False)

class LoadAnimeData(luigi.Task):

    current_timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')

    get_current_timestamp = luigi.Parameter(default=current_timestamp)

    def requires(self):
        return TransformAnimeData()

    def output(self):
        return luigi.LocalTarget(f"load/load_anime_data_{self.get_current_timestamp}.csv")

    def run(self):
        df = pd.read_csv(self.input().path)
        df.to_csv(self.output().path, index=False)

class ExtractMangaData(luigi.Task):

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget("Raw/extract_manga_data.csv")

    def run(self):
        dict_df = getMangaByNum(5)
        if dict_df:
            df = pd.DataFrame(dict_df).T
        # After all data is collected, save to CSV

        df.to_csv(self.output().path, index=False)

class TransformMangaData(luigi.Task):

    def requires(self):
        return ExtractMangaData()

    def output(self):
        return luigi.LocalTarget("transform/transform_manga_data.csv")

    def run(self):
        df = pd.read_csv(self.input().path)
        df = handling_manga(df)
        print(df)
        df.to_csv(self.output().path, index=False)

class LoadMangaData(luigi.Task):

    current_timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')

    get_current_timestamp = luigi.Parameter(default=current_timestamp)

    def requires(self):
        return TransformMangaData()

    def output(self):
        return luigi.LocalTarget(f"load/load_manga_data_{self.get_current_timestamp}.csv")

    def run(self):
        df = pd.read_csv(self.input().path)
        df.to_csv(self.output().path, index=False)

class LoadRecommenderSystemData(luigi.Task):
    def requires(self):
        return [TransformAnimeData(),
                TransformMangaData()]

    def output(self):
        return [luigi.LocalTarget("load/load_anime_data.csv"),
                luigi.LocalTarget("load/load_manga_data.csv")]

    def run(self):
        # init postgres engine
        engine = engine_new()

        # read data from previous task
        anime_data = pd.read_csv(self.input()[0].path)
        manga_data = pd.read_csv(self.input()[1].path)
        print("Anime data path:", self.input()[0].path)
        print("Manga data path:", self.input()[1].path)
        print("Anime data sample:", anime_data.head())
        print("Manga data sample:", manga_data.head())

        # init table name
        anime_data_table = "anime_data"
        manga_data_table = "manga_data"

        # insert to database
        anime_data.to_sql(name = anime_data_table,
                          con = engine,
                          if_exists = "replace",
                          index = False)

        manga_data.to_sql(name = manga_data_table,
                          con = engine,
                          if_exists = "replace",
                          index = False)

        # save the process
        anime_data.to_csv(self.output()[0].path, index = False)
        manga_data.to_csv(self.output()[1].path, index = False)