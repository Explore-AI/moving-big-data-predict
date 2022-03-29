# Data Engineering – Moving Big Data Predict 
© Explore Data Science Academy

## Predict overview


<p align='center'>
     <img src="figs/end-to-end-pipeline.jpg"
     alt='Figure 1: Completed data pipeline'
     width=1000px/>
     <br>
     <em>Figure 1: A representation of the completed pipeline that you will need to implement within this predict.</em>
</p>

Data plays an extremely important role in the study of publicly listed companies and stock market analyses. You're part of a small team tasked with migrating an on-premise application to the AWS cloud. This application shows valuable information about the share prices of thousands of companies to a group of professional stock market traders. The data is further used by data scientists to gain insights into the performance of various companies and predict share price changes that are fed back to the stock market traders.

As part of this migration, raw data, given in the form of company `csv` snapshot batches gathered by the on-premise application, needs to be moved to the AWS cloud. As an initial effort in this movement of data, the stock traders have homed in on the trading data of 1000 companies over the past ten years, and will only require a subset of the raw data to be migrated to the cloud in a batch-wise manner.  

With the above requirements, your role as a data engineer is to create a robust data pipeline that can extract, transform, and load the `csv` data from the source data system to a SQL-based database within AWS. The final pipeline (represented in *Figure 1*) needs to be built in [AWS Data Pipeline](https://aws.amazon.com/datapipeline/) and should utilise AWS services as its functional components. 

### Instructions 🧑‍🏫
A detailed set of instructions guiding you through the creation of the aforementioned data pipeline can be found [here](moving_big_data_predict_student_instructions.md).

### Resources 📕

The resources below are provided in order to help complete the predict.

**Code:**

- [Jupyter Notebook: Python data processing walkthrough](code/python_data_processing_walkthrough.ipynb)
- [Event-based lambda function](code/lambda_pipeline_event.py)

### Assessments ⏱
The following actions need to be taken in order to complete the assessments used within the predict:
- [ ] Create the python processing script from the given notebook
- [ ] Create an AWS Data Pipeline and generate failure/success SNS email alerts
- [ ] Sumbit a CSV containing your student and AWS resource details as described in the instructions 
- [ ] Complete Data Pipeline MCQ
- [ ] Complete Python Processing MCQ

## FAQ ❓

This section of the repo will be periodically updated to represent common questions which may arise around its use. If you detect any problems/bugs, please [create an issue](https://help.github.com/en/github/managing-your-work-on-github/creating-an-issue) and we will do our best to resolve it as quickly as possible.

We wish you all the best in your learning experience 🚀!

<p align='center'>
     <img src="figs/EDSA_logo.png"
     alt='EDSA-logo'
     width=450px/>
     <br>
</p>

