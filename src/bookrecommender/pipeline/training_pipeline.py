from bookrecommender.components.stage_01_data_ingestion import DataIngestion

class TrainingPipeline:
    def __init__(self):
        self.data_ingestion = DataIngestion()

    def start_training_pipeline(self):
        self.data_ingestion.initiate_data_ingestion()
        