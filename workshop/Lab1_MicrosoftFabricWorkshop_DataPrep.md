# Lab1: Create and use Dataflows (Gen2) in Microsoft Fabric

In Microsoft Fabric, Dataflows (Gen2) connect to various data sources and perform transformations in Power Query Online. They can then be used in Data Pipelines to ingest data into a lakehouse or other analytical store, or to define a dataset for a Power BI report.

This lab is designed to introduce the different elements of Dataflows (Gen2), and not create a complex solution that may exist in an enterprise. This lab takes **approximately 40 minutes** to complete.

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
   
2. Select the existing cell in the notebook, which contains some simple code, and then replace the default code with the following variable declaration.

```python
# parameter cell
table_name = "sales"
```

3. In the … menu for the cell (at its top-right) select **Toggle parameter cell**. This configures the cell so that the variables declared in it are treated as parameters when running the notebook from a pipeline.
   
4. Under the parameters cell, use the **+ Code** button to add a new code cell. Then add the following code to it:

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

## Create a Dataflow (Gen2) to ingest data

1. In the lakehouse, select **Get data** > **New Dataflow Gen2**.

2. Name it **Ingest Orders Dataflow**.

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

9. Select **Publish** to publish the dataflow. Then wait for the Dataflow 1 dataflow to be created in your workspace.
---

# Modify the Pipeline

Now that you’ve implemented a notebook to transform data and load it into a table, you can incorporate the notebook into a pipeline to create a reusable ETL process.

1. In the hub menu bar on the left, select the **Ingest Sales Data** pipeline you created previously.

2.  On the **Activities** tab, in the **All activities** list, select **Delete data**.
- Position the new **Delete data** activity to the left of the **Copy data** activity and connect its **On completion** output to the **Copy data** activity, as shown here:

![Dataflow activity](https://microsoftlearning.github.io/mslearn-fabric/Instructions/Labs/Images/delete-data-activity.png)

3. Select the **Delete data** activity. and in the pane below the design canvas, set the following properties:

**General:**
- **Name:** `Delete old files`

**Source:**
- **Connection:** Your lakehouse
- **File path type:** Wildcard file path
- **Folder path:** `Files / new_data`
- **Wildcard file name:** `*.csv`
- **Recursively:** Selected

**Logging settings:**
- **Enable logging:** Unselected

These settings will ensure that any existing `.csv` files are deleted before copying the `sales.csv` file.

4. In the pipeline designer, on the **Activities** tab, select **Notebook** to add a **Notebook activity** to the pipeline.

5. Select the **Copy data** activity and connect its **On Completion** output to the **Notebook activity** as shown here:

![Dataflow activity](https://microsoftlearning.github.io/mslearn-fabric/Instructions/Labs/Images/pipeline.png)

6.  Select the **Notebook** activity, and then in the pane below the design canvas, set the following properties:

**General:**
- **Name:** `Load Sales notebook`

**Settings:**
- **Notebook:** `Load Sales`
- **Base parameters:** Add a new parameter:

| Name        | Type   | Value      |
|-------------|--------|------------|
| table_name  | String | new_sales  |

The `table_name` parameter will be passed to the notebook and override the default value assigned to the `table_name` variable in the parameters cell.

You can include a dataflow as an activity in a pipeline. Pipelines are used to orchestrate data ingestion and processing activities, enabling you to combine dataflows with other kinds of operation in a single, scheduled process. Pipelines can be created in a few different experiences, including Data Factory experience.

7. On the **Activities** tab, in the **All activities** list, select **Dataflow**.
   
8. With the new Dataflow1 activity selected, on the Settings tab, in the Dataflow drop-down list, select **Ingest Orders Dataflow** (the data flow you created previously)
    
9. On the **Home** tab, use the 🖫 (**Save**) icon to save the pipeline. Then use the ▷ (**Run**) button to run the pipeline and wait for all of the activities to complete.

![Dataflow activity](https://microsoftlearning.github.io/mslearn-fabric/Instructions/Labs/Images/pipeline-run.png)
### Note

10. In the hub menu bar on the left edge of the portal, select your lakehouse.
   
11. In the **Explorer** pane, expand **Tables** and select the `new_sales` table to see a preview of the data it contains.

This table was created by the notebook when it was run by the pipeline.

---
## Clean up resources

If you're done:

1. Go to Microsoft Fabric.
2. Select your workspace.
3. Open **Workspace settings** > **Remove this workspace**.
4. Confirm by selecting **Delete**.

---

You have completed the exercise!

