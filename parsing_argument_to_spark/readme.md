---
**This section contain how to parse argument or variable from airflow to spark (pyspark)**

There are only one prerequisite to run the program: setting Variables in airflow. If you don't want to do it, you can change `today_year` source using simple integer or string like "2025".

Here is how to setting Variables in airflow:

![Alt Text](pic/airflow_1.png)
Figure 1

``` python
from airflow.models import Variable

today_year = Variable.get("global_tahun") if bool(int(Variable.get("global_variabel"))) else str(datetime.today().year)

task_2 = SparkSubmitOperator(
    task_id = "run_spark", # task name
    application = "/mydir/spark_parsing_from_airflow.py", # select python file to be executed by spark
    dag = dag,
    application_args=[
        "--tahun", today_year # send argument (today_year) to spark as "tahun", we will call it back in pyspark
    ],
    yarn_queue = "default"
)
```

After we send the argument, receive the argument by look at this section: [Receive argument from Airflow in PySpark](https://github.com/MuhammadMukhlis220/Spark/tree/main/spark_parsing_from_airflow)
