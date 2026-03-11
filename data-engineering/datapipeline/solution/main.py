from pipeline import run_pipeline

if __name__ == "__main__":
    input_folder = "data-engineering/datapipeline/source-data"
    output_folder = "data-engineering/datapipeline/results"
    run_pipeline(input_folder, output_folder)
