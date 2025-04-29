
# Lab3: Data Science in Microsoft Fabric

In this lab, you’ll ingest data, explore the data in a notebook, process the data with the Data Wrangler, and train two types of models.  
By completing this lab, you’ll gain hands-on experience in machine learning and model tracking, and learn how to work with notebooks, Data Wrangler, experiments, and models in Microsoft Fabric.

**Time**: ~40 minutes

---

## Access a lakehouse

1. Navigate to [Microsoft Fabric home page](https://app.fabric.microsoft.com/home?experience=fabric) and sign in.
2. Select **Workspaces** from the left menu.
3. Open your workspace to work on this Lab contents.
4. Select your Lakehouse that you created in Day1, If you didn't create please go the next step
5. (Optional) Select **Create** > **Lakehouse**.
2. (Optional) Give it a unique name: **`<FirstName>_<LastName>_Lakehouse`** unselect "Lakehouse schemas (Public Preview)" option

> **Note**: If the **Create** option isn't pinned, select the ellipsis (`...`) first.

(Optional) After a minute or so, a new empty lakehouse will be created.

![Lakehouse](https://microsoftlearning.github.io/mslearn-fabric/Instructions/Labs/Images/new-lakehouse.png)

---

## Create a notebook

To run code, you can create a notebook. Notebooks provide an interactive environment in which you can write and run code (in multiple languages).

1. On the left menu, select **Create** > **Notebook** under **Data Science**. Give it a unique name: **`<FirstName>_<LastName> Data Science`** 
   - If "Create" isn't visible, select the **ellipsis (…)** first.
   - After a few seconds, a new notebook containing a single cell will open. Notebooks are made up of one or more cells that can contain code or markdown (formatted text).
2. Click the **Add data items** in Explorer panel and select **Existing data sources** and select **Your Lakehouse** to connect
3. Select the first cell (which is currently a code cell), and then in the dynamic tool bar at its top-right, use the M↓ button to convert the cell to a markdown cell.
   - When the cell changes to a markdown cell, the text it contains is rendered.

4. Use the 🖉 (Edit) button to switch the cell to editing mode, then delete the content and enter the following text:

    ```markdown
    # Data science in Microsoft Fabric
    ```

---

## Get the data

Now you’re ready to run code to get data and train a model. You’ll work with the diabetes dataset from the Azure Open Datasets. After loading the data, you’ll convert the data to a Pandas dataframe: a common structure for working with data in rows and columns.

1. Add a new **Code** cell and paste:

> **Note**: To see the + Code icon, move the mouse to just below and to the left of the output from the current cell. Alternatively, in the menu bar, on the Edit tab, select + Add code cell.

  ```python
  # Azure storage access info for open dataset diabetes
  blob_account_name = "azureopendatastorage"
  blob_container_name = "mlsamples"
  blob_relative_path = "diabetes"
  blob_sas_token = r"" # Blank since container is Anonymous access

  # Set Spark config to access blob storage
  wasbs_path = f"wasbs://%s@%s.blob.core.windows.net/%s" % (blob_container_name, blob_account_name, blob_relative_path)
  spark.conf.set("fs.azure.sas.%s.%s.blob.core.windows.net" % (blob_container_name, blob_account_name), blob_sas_token)
  print("Remote blob path: " + wasbs_path)

  # Spark read parquet
  df = spark.read.parquet(wasbs_path)
  ```

2. Run the cell (▶ or `Shift+Enter`).
3. Add another **Code** cell and enter:

    ```python
    display(df)
    ```

4. Review the table output.　When the cell command has completed, review the output below the cell, which should look similar to this:

  | AGE | SEX | BMI  | BP   | S1  | S2   | S3  | S4 | S5    | S6 | Y   |
  |-----|-----|------|------|-----|------|-----|----|-------|----|-----|
  | 59  | 2   | 32.1 | 101.0| 157 | 93.2 | 38.0| 4  | 4.8598| 87 | 151 |
  | 48  | 1   | 21.6 | 87.0 | 183 |103.2 | 70.0| 3  | 3.8918| 69 | 75  |
  | 72  | 2   | 30.5 | 93.0 | 156 | 93.6 | 41.0| 4  | 4.6728| 85 | 141 |
  | 24  | 1   | 25.3 | 84.0 | 198 |131.4 | 40.0| 5  | 4.8903| 89 | 206 |
  | 50  | 1   | 23.0 |101.0 | 192 |125.4 | 52.0| 4  | 4.2905| 80 | 135 |
  | …   | …   | …    | …    | …   | …    | …   | …  | …     | …  | …   |


  ```python
#AGE	Age in years
#SEX	Sex (1 = male, 2 = female)
#BMI	Body Mass Index (weight/height²)
#BP	Average blood pressure
#S1	Total serum cholesterol
#S2	Low-Density Lipoprotein (LDL cholesterol)
#S3	High-Density Lipoprotein (HDL cholesterol)
#S4	Total serum cholesterol / HDL cholesterol
#S5	Logarithm of serum triglycerides
#S6	Blood sugar level (glucose)
#Y	Quantitative measure of diabetes disease progression after one year
  ```
The output shows the rows and columns of the diabetes dataset.

5. There are two tabs at the top of the rendered table: Table and + New chart. Select + New chart.
6. Select the Build my own option at the right of the chart to create a new visualization.
7. Select the following chart settings
   - Chart Type: **Box plot**
   - Y-axis: **Y**
8. Review the output that shows the distribution of the label column **Y**.
---

## Prepare the data

Now that you have ingested and explored the data, you can transform the data. You can either run code in a notebook, or use the Data Wrangler to generate code for you.

1. The data is loaded as a Spark dataframe. While the Data Wrangler accepts either Spark or Pandas dataframes, it is currently optimized to work with Pandas. Therefore, you will convert the data to a Pandas dataframe. Run the following code in your notebook:

    ```python
    df = df.toPandas()
    df.head()
    ```
2. Select **Data Wrangler** in the notebook ribbon, and then select the df dataset. When Data Wrangler launches, it generates a descriptive overview of the dataframe in the **Summary** panel.

Currently, the label column is `Y`, which is a continuous variable. To train a machine learning model that predicts Y, you need to train a regression model. The (predicted) values of Y may be difficult to interpret. Instead, we could explore training a classification model which predicts whether someone is low risk or high risk for developing diabetes. To be able to train a classification model, you need to create a binary label column based on the values from `Y`.

3. Select the `Y` column in the Data Wrangler. Note that there is a decrease in frequency for the `220-240` bin. The 75th percentile `211.5` roughly aligns with transition of the two regions in the histogram. Let’s use this value as the threshold for low and high risk.

4. Navigate to the **Operations** panel, expand **Formulas**, and then select **Create column from formula**.

5. Create a new column with the following settings
     - Column name: `Risk`
     - Formula:

    ```python
    (df['Y'] > 211.5).astype(int)
    ```

5. Apply and add generated code back to the notebook.
6. Run the cell with the code that is generated by Data Wrangler.
7. Run the following code in a new cell to verify that the `Risk` column is shaped as expected:

    ```python
    df_clean.describe()
    ```

---

## Train machine learning models

Now that you’ve prepared the data, you can use it to train a machine learning model to predict diabetes. We can train two different types of models with our dataset: a regression model (predicting `Y`) or a classification model (predicting `Risk`). You’ll train the models using the scikit-learn library and track the models with MLflow.

### Train a regression model

1. Run the following code to split the data into a training and test dataset, and to separate the features from the label `Y` you want to predict:

    ```python
    from sklearn.model_selection import train_test_split

    X, y = df_clean[['AGE','SEX','BMI','BP','S1','S2','S3','S4','S5','S6']].values, df_clean['Y'].values

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
    ```
2. Add another new code cell to the notebook, enter the following code in it, and run it:
    ```python
    import mlflow
    experiment_name = "<FirstName>-experiment-diabetes"
    mlflow.set_experiment(experiment_name)
    ```

    The code creates an MLflow experiment named **your unique identifier experiment-diabetes**. Your models will be tracked in this experiment.

3. Add another new code cell to the notebook, enter the following code in it, and run it:

    ```python
    from sklearn.linear_model import LinearRegression
        
    with mlflow.start_run():
       mlflow.autolog()
        
       model = LinearRegression()
       model.fit(X_train, y_train)
    ```
The code trains a regression model using Linear Regression. Parameters, metrics, and artifacts, are automatically logged with MLflow. Additionally, you’re logging a parameter called **estimator** with the value LinearRegression.

## Train a classification model

1. Run the following code to split the data into a training and test dataset, and to separate the features from the label `Risk` you want to predict:

    ```python
    Xc, yc = df_clean[['AGE','SEX','BMI','BP','S1','S2','S3','S4','S5','S6']].values, df_clean['Risk'].values

    Xc_train, Xc_test, yc_train, yc_test = train_test_split(Xc, yc, test_size=0.3, random_state=42)
    ```

2. Add another new code cell to the notebook, enter the following code in it, and run it:

    ```python
    import mlflow
    experiment_name = "<FirstName>-diabetes-classification"
    mlflow.set_experiment(experiment_name)
    ```
The code creates an MLflow experiment named **your unique identifier diabetes-classification**. Your models will be tracked in this experiment.
3. Add another new code cell to the notebook, enter the following code in it, and run it:
   ```python
   from sklearn.linear_model import LogisticRegression
        
   with mlflow.start_run():
        mlflow.sklearn.autolog()
    
        model = LogisticRegression(C=1/0.1, solver="liblinear").fit(X_train, y_train)
   ```

The code trains a classification model using Logistic Regression. Parameters, metrics, and artifacts, are automatically logged with MLflow.

4. Add another new code cell to the notebook, enter the following code in it, and run it:

    ```python
    from sklearn.tree import DecisionTreeRegressor
    from mlflow.models.signature import ModelSignature
    from mlflow.types.schema import Schema, ColSpec
    
    mlflow.set_experiment(experiment_name)
    with mlflow.start_run():
        mlflow.autolog(log_models=False)
        model = DecisionTreeRegressor(max_depth=5)
        model.fit(X_train, y_train)
           
        # Define the model signature
        input_schema = Schema([
            ColSpec("integer", "AGE"),
            ColSpec("integer", "SEX"),\
            ColSpec("double", "BMI"),
            ColSpec("double", "BP"),
            ColSpec("integer", "S1"),
            ColSpec("double", "S2"),
            ColSpec("double", "S3"),
            ColSpec("double", "S4"),
            ColSpec("double", "S5"),
            ColSpec("integer", "S6"),
         ])
        output_schema = Schema([ColSpec("integer")])
        signature = ModelSignature(inputs=input_schema, outputs=output_schema)
       
        # Log the model
        mlflow.sklearn.log_model(model, "model", signature=signature)
    ```
    The code trains a classification model using DecisionTreeRegressor.


## Track models with MLflow

1. Navigate to the **Experiment** section of Fabric:
   - Check your saved run and model.

---

## Register the model

You can register a model to the Fabric workspace for production use.

1. Add a new code cell and enter the following code to register the model:

    ```python
    # Get the most recent experiment run
    exp = mlflow.get_experiment_by_name(experiment_name)
    last_run = mlflow.search_runs(exp.experiment_id, order_by=["start_time DESC"], max_results=1)
    last_run_id = last_run.iloc[0]["run_id"]
    
    # Register the model
    print("Registering the model from run :", last_run_id)
    model_uri = "runs:/{}/model".format(last_run_id)
    mv = mlflow.register_model(model_uri, "<FirstName>-diabetes-model")
    print("Name: {}".format(mv.name))
    print("Version: {}".format(mv.version))
    ```
Your model is now saved in your workspace as **your unique identifier diabetes-model**. You can use the browse feature in your workspace to find the model.

## Create a test dataset in a lakehouse

To use the model, you’re going to need a dataset of patient details for whom you need to predict a diabetes diagnosis. You’ll create this dataset as a table in a Microsoft Fabric Lakehouse.

1. Add a new code cell and enter the following code to register the model:

    ```python
    from pyspark.sql.types import IntegerType, DoubleType
    
    # Create a new dataframe with patient data
    data = [
        (62, 2, 33.7, 101.0, 157, 93.2, 38.0, 4.0, 4.8598, 87),
        (50, 1, 22.7, 87.0, 183, 103.2, 70.0, 3.0, 3.8918, 69),
        (76, 2, 32.0, 93.0, 156, 93.6, 41.0, 4.0, 4.6728, 85),
        (25, 1, 26.6, 84.0, 198, 131.4, 40.0, 5.0, 4.8903, 89),
        (53, 1, 23.0, 101.0, 192, 125.4, 52.0, 4.0, 4.2905, 80),
        (24, 1, 23.7, 89.0, 139, 64.8, 61.0, 2.0, 4.1897, 68),
        (38, 2, 22.0, 90.0, 160, 99.6, 50.0, 3.0, 3.9512, 82),
        (69, 2, 27.5, 114.0, 255, 185.0, 56.0, 5.0, 4.2485, 92),
        (63, 2, 33.7, 83.0, 179, 119.4, 42.0, 4.0, 4.4773, 94),
        (30, 1, 30.0, 85.0, 180, 93.4, 43.0, 4.0, 5.3845, 88)
    ]
    columns = ['AGE','SEX','BMI','BP','S1','S2','S3','S4','S5','S6']
    df = spark.createDataFrame(data, schema=columns)
    
    # Convert data types to match the model input schema
    df = df.withColumn("AGE", df["AGE"].cast(IntegerType()))
    df = df.withColumn("SEX", df["SEX"].cast(IntegerType()))
    df = df.withColumn("BMI", df["BMI"].cast(DoubleType()))
    df = df.withColumn("BP", df["BP"].cast(DoubleType()))
    df = df.withColumn("S1", df["S1"].cast(IntegerType()))
    df = df.withColumn("S2", df["S2"].cast(DoubleType()))
    df = df.withColumn("S3", df["S3"].cast(DoubleType()))
    df = df.withColumn("S4", df["S4"].cast(DoubleType()))
    df = df.withColumn("S5", df["S5"].cast(DoubleType()))
    df = df.withColumn("S6", df["S6"].cast(IntegerType()))
    
    # Save the data in a delta table
    table_name = "diabetes_test"
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    print(f"Spark dataframe saved to delta table: {table_name}")
    ```

3. Run the code cell.

4. When the code has completed, select the … next to the Tables in the Lakehouse explorer pane, and select Refresh. The diabetes_test table should appear.
5. Expand the diabetes_test table in the left pane to view all fields it includes.

## Create a test dataset in a lakehouse

Now you can use the model you trained previously to generate diabetes progression predictions for the rows of patient data in your table.

1. Add a new code cell and run the following code:
    ```python
    import mlflow
    from synapse.ml.predict import MLFlowTransformer
    
    ## Read the patient features data 
    df_test = spark.read.format("delta").load(f"Tables/{table_name}")
    
    # Use the model to generate diabetes predictions for each row
    model = MLFlowTransformer(
        inputCols=["AGE","SEX","BMI","BP","S1","S2","S3","S4","S5","S6"],
        outputCol="predictions",
        modelName="diabetes-model",
        modelVersion=1)
    df_test = model.transform(df)
    
    # Save the results (the original features PLUS the prediction)
    df_test.write.format('delta').mode("overwrite").option("mergeSchema", "true").saveAsTable(table_name)
    ```
2. After the code has finished, select the … next to the diabetes_test table in the Lakehouse explorer pane, and select Refresh. A new field predictions has been added.
3. Add a new code cell to the notebook and drag the diabetes_test table to it. The necessary code to view the table’s contents will appear. Run the cell to display the data.
---

# Congratulations!

You have now:
- Ingested and explored data.
- Cleaned and prepared data with Data Wrangler.
- Trained a machine learning model to predict diabetes progression,
- Deployed and registered the model,
- Created test data in a Lakehouse,
- Used the model to generate batch predictions in Microsoft Fabric.

---

# References

- [Microsoft Fabric Documentation](https://learn.microsoft.com/fabric/)
- [Get a free Microsoft Fabric trial](https://learn.microsoft.com/fabric/get-started/fabric-trial)
