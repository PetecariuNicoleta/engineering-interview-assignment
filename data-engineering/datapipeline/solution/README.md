# Solution README

## Solution descripton

This pipeline reads the input files from the 'source-data' folder. After that we applie some transformations to the data, and then we create the output files into the 'result' folder. 
The programming language used to write this solution is Python.

## Requirments met:
Core requirments:
- Read the input data 
- Process and tranform the data
- Generate output files in the targeted folder.

## Stretch requirments:
- I added some basic unit tests using the framework pytest. 
- Added some input validation
- Split the code into a much readable format creating the utils.py file where the data cleaning frunction is defined.
- If this pipeline needs to be deployed to the cloud,first i would choose a data platform such as databricks with azure data lake storage. The data would first be ingested into the data lake using an orchestration tools(airflow or azure data factory). 
The transformation logic written in pySpark could then run as scheduled job in databricks, reading raw data from the data lake, processing it, and writing the results to the tables following the medallion architecture(bronze, silver, gold layer). 
For deployment and version control i would store the code on Git where if the pipeline si small we can do manual deployment but if it is more complex we cand use a CI/CD pipeline for the automation of the deployment into cloud. 

I would like to mention this several important aspects that needs to be taken in consideration before deploying the pipeline, and some question that i try to ask myself with every pipeline that i buid:

- Scalability (What happens if the dataset grows? Can this pipeline handle more data in the future?)
- Cost management (How does the cost grow with more data or more frequent runs?)
- Security (Who can access the data and the pipeline?)
- Monitoring (How do we know if the pipeline ran successfully?)

## How to run the pipeline:
1. We can run the python3 data-engineering/datapipeline/solution/main.py command witch will start the pipeline
2. To run the unit tests you can run the python3 -m pytest command in the root folder of the project. I have configured the project path so the test modules can import the pipeline utilities.




