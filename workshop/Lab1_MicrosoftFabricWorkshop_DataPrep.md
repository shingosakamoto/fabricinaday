# Lab1: Create and use Dataflows (Gen2) in Microsoft Fabric

In Microsoft Fabric, Dataflows (Gen2) connect to various data sources and perform transformations in Power Query Online. They can then be used in Data Pipelines to ingest data into a lakehouse or other analytical store, or to define a dataset for a Power BI report.

This lab is designed to introduce the different elements of Dataflows (Gen2), and not create a complex solution that may exist in an enterprise. This lab takes **approximately 30 minutes** to complete.

> **Note**: You need a [Microsoft Fabric trial](https://learn.microsoft.com/fabric/get-started/fabric-trial) to complete this exercise.

---

## Create a workspace

Before working with data in Fabric, create a workspace with the Fabric trial enabled.

1. Navigate to the [Microsoft Fabric home page](https://app.fabric.microsoft.com/home?experience=fabric) and sign in.
2. In the menu bar on the left, select Workspaces (the icon looks similar to 🗇).
3. Create a new workspace with a name of your choice (e.g. **`<FirstName>_<LastName>_Workspace`**), selecting a licensing mode that includes Fabric capacity (Trial, Premium, or Fabric).

   ![Workspace](https://microsoftlearning.github.io/mslearn-fabric/Instructions/Labs/Images/new-workspace.png)

---

## Create a lakehouse

Now that you have a workspace, create a lakehouse to ingest data.

1. Select **Create** > **Lakehouse**.
2. Give it a unique name: **`<FirstName>_<LastName>_Lakehouse`** unselect "Lakehouse schemas (Public Preview)" option

> **Note**: If the **Create** option isn't pinned, select the ellipsis (`...`) first.

After a minute or so, a new empty lakehouse will be created.

![Lakehouse](https://microsoftlearning.github.io/mslearn-fabric/Instructions/Labs/Images/new-lakehouse.png)

---

## Create a pipeline

A simple way to ingest data is to use a **Copy Data** activity in a pipeline to extract the data from a source and copy it to a file in the lakehouse.

1. On the **Home** page for your lakehouse, select **Get data** and then select **New data pipeline**, and create a new data pipeline named **Ingest Sales Data**.
> **Note**: If the **Copy Data** wizard doesn’t open automatically, select Copy Data > Use copy assistant in the pipeline editor page.
2. In the **Copy Data** wizard, on the **Choose data source** page, type HTTP in the search bar and then select **HTTP** in the **New sources** section.

In the wizard:

- **Choose data source**: Search "HTTP" > select **HTTP**.

   ![Choose data source](https://microsoftlearning.github.io/mslearn-fabric/Instructions/Labs/Images/choose-data-source.png)

3. In the Connect to data source pane, enter the following settings for the connection to your data source
 - **Url**: `https://raw.githubusercontent.com/MicrosoftLearning/dp-data/main/sales.csv`
 - **Connection**: Create new connection
 - **Connection name**: Specify a unique name
 - **Data gateway**: (none)
 - **Authentication**: Anonymous
 - Unselect the gateway option

4. Select **Next**. Then ensure the following settings are selected:
   Choose data source
 - Relative URL: Leave blank
 - Request method: GET
 - Additional headers: Leave blank
 - Binary copy: Unselected
 - Request timeout: Leave blank
 - Max concurrent connections: Leave blank

5. Select **Next**, and wait for the data to be sampled and then ensure that the following settings are selected:
   Connect to data source
 - **File format**: DelimitedText
 - **Column delimiter**: Comma
 - **Row delimiter**: Line feed (`\n`)
 - **First row as header**: Selected
 - **Compression type**: None

6. Select Preview data to see a sample of the data that will be ingested. Then close the data preview and select Next.
7. On the Connect to data destination page, set the following data destination options, and then select Next
- Destination:
  - Root folder: Files
  - Folder path: `new_data`
  - File name: `sales.csv`
  - Copy behavior: None
8. Set the following file format options and then select **Next**:
 - File format: DelimitedText
 - Column delimiter: Comma (,)
 - Row delimiter: Line feed (\n)
 - Add header to file: Selected
 - Compression type: None
 - Save and run the pipeline.

9. On the **Copy summary** page, review the details of your copy operation and select "Start data transfer immediately" option then select **Save + Run**.
After the run, go to your lakehouse > **Files** > `new_data` folder to confirm `sales.csv` exists.

![New Copy Data](https://microsoftlearning.github.io/mslearn-fabric/Instructions/Labs/Images/copy-data-pipeline.png)
10. When the pipeline starts to run, you can monitor its status in the **Output** pane under the pipeline designer. Use the ↻ (Refresh) icon to refresh the status, and wait until it has succeeeded.

11. In the menu bar on the left, select your lakehouse.
12. On the Home page, in the Lakehouse explorer pane, expand Files and select the new_data folder to verify that the sales.csv file has been copied.
---



## Create a notebook and load external data
Create a new Fabric notebook and connect to external data source with PySpark.
1. From the top menu in the lakehouse, select **Open notebook** > **New notebook**.

   After a few seconds, a new notebook containing a single cell will open. Notebooks are made up of one or more cells that can contain code or markdown (formatted text).
   
3. Select the existing cell in the notebook, which contains some simple code, and then replace the default code with the following variable declaration.

```python
# parameter cell
table_name = "sales"
```
3. In the … menu for the cell (at its top-right) select **Toggle parameter cell**. This configures the cell so that the variables declared in it are treated as parameters when running the notebook from a pipeline.
   
5. Under the parameters cell, use the **+ Code** button to add a new code cell. Then add the following code to it:

```python
from pyspark.sql.functions import *

df = spark.read.format("csv").option("header", "true").load("Files/new_data/sales.csv")
df = df.withColumn("Year", year(col("OrderDate"))).withColumn("Month", month(col("OrderDate")))
df = df.withColumn("FirstName", split(col("CustomerName"), " ").getItem(0)).withColumn("LastName", split(col("CustomerName"), " ").getItem(1))
df = df[["SalesOrderNumber", "SalesOrderLineNumber", "OrderDate", "Year", "Month", "FirstName", "LastName", "EmailAddress", "Item", "Quantity", "UnitPrice", "TaxAmount"]]
df.write.format("delta").mode("append").saveAsTable(table_name)
```
This code loads the data from the sales.csv file that was ingested by the Copy Data activity, applies some transformation logic, and saves the transformed data as a table - appending the data if the table already exists.

5. Verify that your notebooks looks similar to this, and then use the ▷ **Run all** button on the toolbar to run all of the cells it contains.
![New Code](https://microsoftlearning.github.io/mslearn-fabric/Instructions/Labs/Images/notebook.png)



> **Note**: Since this is the first time you’ve run any Spark code in this session, the Spark pool must be started. This means that the first cell can take a minute or so to complete.
6. When the notebook run has completed, in the **Lakehouse explorer** pane on the left, in the … menu for **Tables** select **Refresh** and verify that a **sales** table has been created.

7. In the notebook menu bar, use the ⚙️ **Settings** icon to view the notebook settings. Then set the Name of the notebook to **Load Sales** and close the settings pane.

8. In the hub menu bar on the left, select your lakehouse.

9. In the **Explorer** pane, refresh the view. Then expand Tables, and select the **sales** table to see a preview of the data it contains.

---

## Add a dataflow to a pipeline

1. Create a **New data pipeline** > Name: **Load data**.
2. Add a **Dataflow** activity and select your Dataflow.
3. Save and run the pipeline.

![Dataflow activity](./Create%20and%20use%20Dataflows%20(Gen2)%20in%20Microsoft%20Fabric%20_%20mslearn-fabric_files/dataflow-activity.png)

4. Confirm a table (**orders**) is created in your lakehouse.

---

## Modify the pipeline

Enhance your pipeline to:

- Delete old files
- Load data using a Notebook

### Add Delete Data Activity

1. Add **Delete data** activity before **Copy data**.
2. Configure:
   - Folder: `Files/new_data`
   - Wildcard file name: `*.csv`
   - Recursively: Selected

### Add Notebook Activity

1. Add **Notebook** activity after **Copy data**.
2. Configure:
   - Notebook: **Load Sales**
   - Base parameters:

| Name       | Type   | Value      |
|------------|--------|------------|
| table_name | String | new_sales  |

3. Save and run the pipeline.

> **Note**: If you encounter a "lakehouse context" error, reattach your Lakehouse to the Notebook.

4. Verify a **new_sales** table is created.

---
## Create a Dataflow (Gen2) to ingest data

1. In the lakehouse, select **Get data** > **New Dataflow Gen2**.
2. Name it **`<FirstName>_<LastName>_Dataflow`**.

![New dataflow](https://microsoftlearning.github.io/mslearn-fabric/Instructions/Labs/Images/new-dataflow.png)

3. Select **Import from a Text/CSV file** and create a new data source with the following settings:
4. Configure:
   - **Link to file**: Selected
   - **File path or URL**: `https://raw.githubusercontent.com/MicrosoftLearning/dp-data/main/orders.csv`
   - **Connection**: Create new connection
   - **Data gateway**: (none)
   - **Authentication kind**: Organizational account

5. Select **Next** to preview the file data, and then **Create** the data source. The Power Query editor shows the data source and an initial set of query steps to format the data, as shown here:

![Custom column](https://microsoftlearning.github.io/mslearn-fabric/Instructions/Labs/Images/power-query.png)

6. On the toolbar ribbon, select the **Add column** tab. Then select **Custom column** and create a new column.
Create a **Custom column**:
   - New column name: `MonthNo`
   - Data type: Whole Number
   - Formula: `Date.Month([OrderDate])`
> **Tip**: Check the Query Settings > Applied Steps pane.

![Custom column](https://microsoftlearning.github.io/mslearn-fabric/Instructions/Labs/Images/custom-column.png)

7. Select OK to create the column and notice how the step to add the custom column is added to the query. The resulting column is displayed in the data pane:

![Custom column](https://microsoftlearning.github.io/mslearn-fabric/Instructions/Labs/Images/custom-column-added.png)

8. Check and confirm that the data type for the **OrderDate** column is set to **Date** and the data type for the newly created column **MonthNo** is set to **Whole Number**.
---

## Confirm the data destination for Dataflow

1. Check that the data destination is set (lakehouse destination indicated).
2. Publish the Dataflow.

![Lakehouse destination](./Create%20and%20use%20Dataflows%20(Gen2)%20in%20Microsoft%20Fabric%20_%20mslearn-fabric_files/lakehouse-destination.png)

---
## Clean up resources

If you're done:

1. Go to Microsoft Fabric.
2. Select your workspace.
3. Open **Workspace settings** > **Remove this workspace**.
4. Confirm by selecting **Delete**.

---

You have completed the exercise!

