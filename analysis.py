import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine

# This is the link to the database
DATABASE_CON = 'postgresql+psycopg2://postgres:admin@localhost:5432/weather_transport_data'

# Create database connection
engine = create_engine(DATABASE_CON)


# Small function used to group the height of all weather stations.
def groupElevation():
    df_height = pd.read_sql('SELECT weatherstationname, stationheight, canton FROM weatherstation', engine)

    # Classify elevation directly within a new column using a lambda function
    df_height["Elevation Group"] = df_height["stationheight"].apply(
        lambda height: "Low Elevation" if height < 500 else
        "Medium Elevation" if 500 <= height < 1500 else
        "High Elevation"
    )
    return df_height


# This function analyzes weather data for a given month and a specific metric. In more detail it plots a line graph for
# the given metric grouped by three height groups (>500m, 500m - 1500m, >1500m).
# This is not shown in the report and the presentation, however we used this function to find out which values, in
# combination with month, were meaningful enough to further analyze.
def analyzeWeatherData(month, metric):
    # Query to fetch relevant weather and station data
    query = f"""
    SELECT
        ws.StationHeight,
        ws.Canton,
        w.Date,
        w.AirTemperature_mean,
        w.AirTemperature_min,
        w.AirTemperature_max,
        w.Precipitation,
        w.TotalSnowDepth,
        w.SunshineDuration
    FROM
        WeatherStation ws
    JOIN
        Weather w ON ws.WeatherStationName = w.WeatherStationName
    WHERE
        EXTRACT(MONTH FROM w.Date) = {month};
    """

    # Load weather data into a DataFrame
    weather_data = pd.read_sql_query(query, engine)

    # Call the groupElevation function to classify height
    df_height = groupElevation()
    merged_data = pd.merge(weather_data, df_height[['canton', 'Elevation Group']], on='canton', how='left')

    # Group data by elevation group and date, calculate the average for the metric
    grouped_data = (
        merged_data.groupby(['Elevation Group', 'date'])[metric]
        .mean()
        .reset_index()
    )

    # Plot a line graph, with a line for each elevation group
    elevation_groups = grouped_data['Elevation Group'].unique()
    plt.figure(figsize=(12, 6))

    for group in elevation_groups:
        group_data = grouped_data[grouped_data['Elevation Group'] == group]
        plt.plot(group_data['date'], group_data[metric], label=f"{group} Group")

    plt.title(f"Daily {metric} in Month {month} by Elevation Group")
    plt.xlabel("Date")
    plt.ylabel(metric)
    plt.legend(title="Elevation Group")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


# This function analyzes the delays by height group for a given month and mode of transport, by displaying it as a
# violin plot.
def analyzeDelayByRegionPerMonthViolin(month, product_id):
    # Query to fetch data
    query = f"""
        SELECT
            ws.StationHeight,
            ws.Canton,
            te.Date,
            EXTRACT(EPOCH FROM (te.DepartureTime - te.ArrivalTime)) / 60 AS DelayMinutes
        FROM
            WeatherStation ws
        JOIN
            Map_To_Transport mt ON ws.WeatherStationName = mt.WeatherStationName
        JOIN
            TransportEvent te ON mt.BPUIC = te.BPUIC
        WHERE
            te.ProduktID = '{product_id}'
            AND EXTRACT(MONTH FROM te.Date) = {month}
            AND EXTRACT(EPOCH FROM (te.DepartureTime - te.ArrivalTime)) / 60 > 0;  -- Exclude negative delays
    """

    # Load data into DataFrame
    delay_data = pd.read_sql_query(query, engine)

    # Add elevation group classification
    df_height = groupElevation()
    merged_data = pd.merge(delay_data, df_height[['canton', 'Elevation Group']], on='canton', how='left')

    # Group delays into three categories
    zero_five_delays = merged_data[merged_data['delayminutes'] < 5]
    five_ten_delays = merged_data[(merged_data['delayminutes'] < 10) & (merged_data['delayminutes'] > 5)]
    # This here ignores all delays over 30 minutes, however after significant testing, we decided that since there are
    # so little occasions where the delay is that large (often times just 1 or 2 times), we ignore this span.
    ten_thirty_delays = merged_data[(merged_data['delayminutes'] < 30) & (merged_data['delayminutes'] > 10)]

    elevation_order = ["Low Elevation", "Medium Elevation", "High Elevation"]

    # Create subplots
    fig, axes = plt.subplots(1, 3, figsize=(18, 6))

    # Plot each violin plot in a separate subplot
    sns.violinplot(
        data=zero_five_delays,
        x='Elevation Group',
        y='delayminutes',
        order=elevation_order,
        palette="muted",
        scale='width',
        ax=axes[0]
    )
    axes[0].set_title("Delays 0-5 minutes")
    axes[0].set_xlabel("Elevation Group")
    axes[0].set_ylabel("Delay (minutes)")

    sns.violinplot(
        data=five_ten_delays,
        x='Elevation Group',
        y='delayminutes',
        order=elevation_order,
        palette="muted",
        scale='width',
        ax=axes[1]
    )
    axes[1].set_title("Delays 5-10 minutes")
    axes[1].set_xlabel("Elevation Group")

    sns.violinplot(
        data=ten_thirty_delays,
        x='Elevation Group',
        y='delayminutes',
        order=elevation_order,
        palette="muted",
        scale='width',
        ax=axes[2]
    )
    axes[2].set_title("Delays 10-30 minutes")
    axes[2].set_xlabel("Elevation Group")

    # Customize layout
    plt.subplots_adjust(top=0.85)
    plt.suptitle(f"Delay Distribution by Elevation Group in Month {month} ({product_id})", fontsize=16)
    plt.show()


# This function plots a heatmap that show the percentage of cancellations in contrast to the mean air temperature and
# the precipitation (includes snow). It plots the information for the entire year and one elevation group.
def plotHeatmap(elevation_data):
    # Create bins for two weather variables (e.g., snow depth and precipitation)
    elevation_data['temperature_group'] = pd.cut(
        elevation_data['airtemperature_mean'],
        bins=[-20, -10, 0, 10, 20, float('inf')],
        labels=["-20 ~ -10", "-10 ~ 0", "0 ~ 10", "10 ~ 20", "over 20"]
    )
    elevation_data['precipitation_group'] = pd.cut(
        elevation_data['precipitation'],
        bins=[0, 5, 20, float('inf')],
        labels=["Low", "Moderate", "High"]
    )

    # Aggregate: Count cancellations in each bin combination
    heatmap_data = (
        elevation_data.groupby(['precipitation_group', 'temperature_group'])
        .agg(TotalTrips=('tid', 'count'), CancelTrips=('faelltaus', 'sum'))
        .reset_index()
    )
    heatmap_data['CancellationPercent'] = (heatmap_data['CancelTrips'] / heatmap_data[
        'TotalTrips']) * 100

    # Pivot the data for the heatmap
    heatmap_pivot = heatmap_data.pivot(
        index='precipitation_group',
        columns='temperature_group',
        values='CancellationPercent'
    )

    # Plot the heatmap
    plt.figure(figsize=(10, 8))
    sns.heatmap(
        heatmap_pivot,
        annot=True,
        cmap="coolwarm",
        fmt=".1f",
        linewidths=0.5,
        cbar_kws={'label': 'Cancellation Percentage'}
    )
    plt.title("Percent of Cancellations Based on Temperature and Precipitation in Mountains")
    plt.xlabel("Temperature Level")
    plt.ylabel("Precipitation Level")
    plt.show()


# This function runs the SQL query for getting the delay times and other information for the entire year for trains.
# This is done as this takes up quite a bit of time, so it only needs to be run once.
def getTrainDataYear():
    query = f"""
            SELECT 
                ws.Canton,
                te.TID,
                te.FaelltAus,
                EXTRACT(EPOCH FROM (te.DepartureTime - te.ArrivalTime))/60 AS AvgDelayMinutes,
                w.totalsnowdepth,
                w.precipitation,
                w.globalradiation,
                w.cloudcover,
                w.pressure,
                w.sunshineduration,
                w.airtemperature_mean,
                w.relativehumidity,
                te.date
            FROM 
                WeatherStation ws
            JOIN 
                Map_To_Transport mt ON ws.WeatherStationName = mt.WeatherStationName
            JOIN 
                TransportEvent te ON mt.BPUIC = te.BPUIC
            Join 
                Weather w ON ws.weatherstationname = w.weatherstationname
                And te.Date = w.Date
            WHERE 
                te.produktid = 'Zug';
                """

    train_data = pd.read_sql_query(query, engine)
    return train_data


# This function is the "main" function for creating the heatmap mentioned in "plotHeatmap()".
def heatmapAnalysisWholeYear(train_data):
    df_height = groupElevation()
    if train_data.empty:
        train_data = getTrainDataYear()

    merged_data = pd.merge(train_data, df_height[['canton', 'Elevation Group']], on='canton', how='left')

    # Filter data for different elevation groups
    high_elevation_data = merged_data[merged_data["Elevation Group"] == "High Elevation"]
    medium_elevation_data = merged_data[merged_data["Elevation Group"] == "Medium Elevation"]
    low_elevation_data = merged_data[merged_data["Elevation Group"] == "Low Elevation"]

    # Plot three different heatmaps
    plotHeatmap(high_elevation_data)
    plotHeatmap(medium_elevation_data)
    plotHeatmap(low_elevation_data)


# Function to calculate time series data for delay percentage for each elevation group for the specified month.
def calculate_delay_percentage_for_month(data, elevation_group, month):
    filtered_data = data[data['date'].dt.month == month]
    time_series_data = (
        filtered_data.groupby(filtered_data['date'].dt.date)
        .agg(
            TotalTrips=('tid', 'count'),
            DelayedTrips=('avgdelayminutes', lambda x: (x > 0).sum())  # Count trips with delay > 0 TODO MAYBE > 1
        )
        .reset_index()
    )
    time_series_data['DelayPercent'] = (time_series_data['DelayedTrips'] / time_series_data['TotalTrips']) * 100
    time_series_data['Elevation Group'] = elevation_group
    return time_series_data


# This function plots the delay percentage per month for all three elevation groups. This is done with a line graph.
def delayPercentageMonth(month, train_data):
    df_height = groupElevation()
    if train_data.empty:
        train_data = getTrainDataYear()

    merged_data = pd.merge(train_data, df_height[['canton', 'Elevation Group']], on='canton', how='left')

    # Filter data for different elevation groups
    high_elevation_data = merged_data[merged_data["Elevation Group"] == "High Elevation"]
    medium_elevation_data = merged_data[merged_data["Elevation Group"] == "Medium Elevation"]
    low_elevation_data = merged_data[merged_data["Elevation Group"] == "Low Elevation"]

    # Ensure the 'date' column is in datetime format
    high_elevation_data['date'] = pd.to_datetime(high_elevation_data['date'])
    medium_elevation_data['date'] = pd.to_datetime(medium_elevation_data['date'])
    low_elevation_data['date'] = pd.to_datetime(low_elevation_data['date'])

    # Calculate time series data for the specified month
    low_series = calculate_delay_percentage_for_month(low_elevation_data, "Low Elevation", month)
    medium_series = calculate_delay_percentage_for_month(medium_elevation_data, "Medium Elevation", month)
    high_series = calculate_delay_percentage_for_month(high_elevation_data, "High Elevation", month)

    # Combine the data
    combined_series = pd.concat([low_series, medium_series, high_series])

    # Plot the line graph
    plt.figure(figsize=(12, 8))
    sns.lineplot(
        data=combined_series,
        x='date',
        y='DelayPercent',
        hue='Elevation Group',
        marker='o',
        linewidth=2
    )

    # Customizing the plot
    plt.title(f"Delay Percentage Over Time (Month: {month})", fontsize=16)
    plt.xlabel("Time", fontsize=12)
    plt.ylabel("Delay Percentage", fontsize=12)
    plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.legend(title="Elevation Group", fontsize=10)

    plt.tight_layout()
    plt.show()


# Function to calculate time series data for cancellation percentage for each elevation group for the specified month.
def calculate_time_series_for_month(data, elevation_group, month):
    filtered_data = data[data['date'].dt.month == month]
    time_series_data = (
        filtered_data.groupby(filtered_data['date'].dt.date)
        .agg(TotalTrips=('tid', 'count'), CancelTrips=('faelltaus', 'sum'))
        .reset_index()
    )
    time_series_data['CancellationPercent'] = (time_series_data['CancelTrips'] / time_series_data[
        'TotalTrips']) * 100
    time_series_data['Elevation Group'] = elevation_group
    return time_series_data


# This function plots the cancellation percentage per month for all three elevation groups. This is done with a line
# graph.
def cancellationPercentageMonth(month, train_data):
    df_height = groupElevation()
    if train_data.empty:
        train_data = getTrainDataYear()

    merged_data1 = pd.merge(train_data, df_height[['canton', 'Elevation Group']], on='canton', how='left')

    # Filter data for different elevation groups
    high_elevation_data = merged_data1[merged_data1["Elevation Group"] == "High Elevation"]
    medium_elevation_data = merged_data1[merged_data1["Elevation Group"] == "Medium Elevation"]
    low_elevation_data = merged_data1[merged_data1["Elevation Group"] == "Low Elevation"]

    # Ensure the 'date' column is in datetime format
    high_elevation_data['date'] = pd.to_datetime(high_elevation_data['date'])
    medium_elevation_data['date'] = pd.to_datetime(medium_elevation_data['date'])
    low_elevation_data['date'] = pd.to_datetime(low_elevation_data['date'])

    # Ensure the 'date' column is in datetime format
    low_elevation_data['date'] = pd.to_datetime(low_elevation_data['date'])
    medium_elevation_data['date'] = pd.to_datetime(medium_elevation_data['date'])
    high_elevation_data['date'] = pd.to_datetime(high_elevation_data['date'])

    # Calculate time series data for the specified month
    low_series = calculate_time_series_for_month(low_elevation_data, "Low Elevation", month)
    medium_series = calculate_time_series_for_month(medium_elevation_data, "Medium Elevation", month)
    high_series = calculate_time_series_for_month(high_elevation_data, "High Elevation", month)

    # Combine the data
    combined_series = pd.concat([low_series, medium_series, high_series])

    # Plot the line graph
    plt.figure(figsize=(12, 8))
    sns.lineplot(
        data=combined_series,
        x='date',
        y='CancellationPercent',
        hue='Elevation Group',
        marker='o',
        linewidth=2
    )

    # Customizing the plot
    plt.title(f"Cancellation Percentage Over Time (Month: {month})", fontsize=16)
    plt.xlabel("Time", fontsize=12)
    plt.ylabel("Cancellation Percentage", fontsize=12)
    plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.legend(title="Elevation Group", fontsize=10)

    plt.tight_layout()
    plt.show()


# Run this to run all the analysis tasks and plots in a row. This will take around 1 hour.
if __name__ == '__main__':
    analyzeDelayByRegionPerMonthViolin(1, 'Zug')
    analyzeDelayByRegionPerMonthViolin(4, 'Zug')
    analyzeDelayByRegionPerMonthViolin(7, 'Zug')
    analyzeDelayByRegionPerMonthViolin(11, 'Zug')
    train_data = getTrainDataYear()
    heatmapAnalysisWholeYear(train_data)
    delayPercentageMonth(1, train_data)
    delayPercentageMonth(4, train_data)
    delayPercentageMonth(7, train_data)
    delayPercentageMonth(11, train_data)
    cancellationPercentageMonth(1, train_data)
    cancellationPercentageMonth(4, train_data)
    cancellationPercentageMonth(7, train_data)
    cancellationPercentageMonth(11, train_data)
