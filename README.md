---
# Airflow Python Branch Operator

**Python Branch Operator di Apache Airflow**
<br>
Apache Airflow adalah platform untuk mendefinisikan, menjadwalkan, dan memonitor workflow. Salah satu fitur yang sangat berguna dalam Airflow adalah BranchPythonOperator, yang memungkinkan kita membuat alur kerja bercabang (branching) di dalam DAG (Directed Acyclic Graph). Dalam tutorial ini, kita akan membahas apa itu BranchPythonOperator, bagaimana cara menggunakannya, dan bagaimana cara membuat alur kerja dengan percabangan berdasarkan kondisi tertentu.

**Apa Itu BranchPythonOperator?**
<br>
BranchPythonOperator adalah jenis operator di Airflow yang memungkinkan kita untuk menentukan alur eksekusi yang berbeda berdasarkan hasil dari fungsi Python yang dijalankan. Operator ini sangat berguna ketika Anda ingin mengeksekusi beberapa task dalam sebuah DAG, namun hanya salah satu task yang perlu dijalankan berdasarkan kondisi tertentu.

Ketika sebuah BranchPythonOperator dijalankan, ia akan mengembalikan nama task berikutnya yang harus dieksekusi, tergantung pada logika yang ditentukan di dalam fungsi Python. Jika lebih dari satu branch dipilih, hanya satu task yang akan dieksekusi, dan sisa task yang tidak dipilih akan dilewati (skipped).

Berikut adalah contoh hasil DAG yang dibuat dimana terdapat 2 cabang task yang akan menjadi pilihan eksekusi antara **good_grade_task** dan **improve_task**

![Alt Text](/pic/preview_1.png)
**Figure 1**

Mari kita bahas cara penggunaannya

## Step 1 Create Airflow DAG 

Seperti biasa, kita akan membuat DAG menggunakan code editor favorit kalian.

For the complete Python programming code, refer to the following block.
<details>
   <summary>Click to view the complete Python code.</summary>

   ```python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime

default_args = {
        'owner': 'mukhlis',
    'start_date': datetime(2025, 2, 12)
}

dag = DAG(
        'branch_operator',
        default_args = default_args,
        catchup = False,
        schedule_interval = None,
        tags = ['test', 'branchoperator', 'python']
)

def generate_value(**kwargs):
    score = 100 
    kwargs['ti'].xcom_push(key='score', value=score)

def branch_logic(**kwargs):
    score = kwargs['ti'].xcom_pull(task_ids='generate_value', key='score')
    
    if score > 75:
        return 'good_grade_task'
    else:
        return 'improve_task'

def good_grade(**kwargs):
    score = kwargs['ti'].xcom_pull(task_ids='generate_value', key='score')
    print(f"Good score, you get score {score} where is enough to pass this grade")

def improve_grade(**kwargs):
    score = kwargs['ti'].xcom_pull(task_ids='generate_value', key='score')
    print(f"Take more lesson for the exam, because your score is {score}")


generate_value_task = PythonOperator(
    task_id='generate_value',
    python_callable=generate_value,
    provide_context=True,
    dag=dag,
)

branch_task = BranchPythonOperator(
    task_id='branch_task',
    python_callable=branch_logic,
    provide_context=True,
    dag=dag,
)

good_grade_task = PythonOperator(
    task_id='good_grade_task',
    python_callable=good_grade,
    provide_context=True,
    dag=dag,
)

improve_task = PythonOperator(
    task_id='improve_task',
    python_callable=improve_grade,
    provide_context=True,
    dag=dag,
)

generate_value_task >> branch_task
branch_task >> [good_grade_task, improve_task]

   ```
   </details>

Dari kode di atas, kita akan melakukan simulasi pemilihan eksekusi cabang task menggunakan `score`. Library yang dibutuhkan adalah BranchPythonOperator. Pemilihan cabang task akan ditentukan dari berapa nilai variabel `score`. KIta menggunakan if-else statement dengan me-return **nama task** dari task yang akan kita pilih. If-else statement ini dibuat di dalam fungsi python bernama `branch_logic`. Kita set untuk nilai `score` yang lebih dari 75 akan me-return task `good_grade_task` dari fungsi yang akan dibuat bernama `good_grade` dan *else*-nya me-return task `improve_task` dari fungsi yang akan dibuat bernama `improve_grade`. Set variabel `score` menjadi 100 untuk mengeksekusi **if** statement.

![Alt Text](/pic/code_1.png)

**Figure 2**

Kemudian kita membuat 2 fungsi python bernama `good_grade` dan `improve_grade` yang keduanya akan menjadi pilihan cabang eksekusi. Jadikan fungsi `branch_logic` sebagai task yang menggunakan **BranchPythonOperator** sebagai fungsi yang memilih percabangan task selanjutnya. **Buat task dengan nama yang sesuai dari hasil return if-else statement yang telah dibuat**.

![Alt Text](/pic/code_2.png)

**Figure 3**

DI urutan eksekusi task, buat task cabang menggunakan karakter kurung kurawal "[]" yang mengurung kedua nama task tersebut.

![Alt Text](/pic/code_3.png)

**Figure 4**

Simpan DAG tersebut dengan nama DAG **branch_operator**

## Step 2 Run the DAG and Check the Result

Eksekusi DAG yang dibuat dan hasilnya adalah task `good_grade` yang dipilih dan task `improve_task` dilewatkan.

![Alt Text](/pic/result_2.png)

**Figure 5**

Jika kita melihat log-nya, ada informasi yang menyatakan **Branch callable return** yang menyatakan `good_grade` task dipilih dan informasi **Skipping tasks [('improve_task', -1)]** yang menyatakan task `improve_task` dilewatkan.

![Alt Text](/pic/result_1.png)

**Figure 6**

<br>
<br>
<br>

**Thats all, give it a try!**
