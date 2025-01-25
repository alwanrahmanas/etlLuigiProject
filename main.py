if __name__ == "__main__":
    luigi.build([ExtractAnimeData(),
             ExtractMangaData(),
             TransformAnimeData(),
             TransformMangaData(),
             LoadRecommenderSystemData()],
             local_scheduler = False)