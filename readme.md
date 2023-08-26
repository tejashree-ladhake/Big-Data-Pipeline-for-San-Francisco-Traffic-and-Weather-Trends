# Big Data Pipeline for San Francisco Traffic and Weather Trends

**How I Used Apache Airflow, Spark, MongoDB and SQL to Analyze Traffic Violations and Weather Data in San Francisco?**

In this blog post, I will share with you how I completed a data engineering project that involved collecting, aggregating, storing and exploring data from two sources: traffic violations and weather data in San Francisco. I will also explain the tools and techniques I used, such as `Apache Airflow, Spark, MongoDB and SQL`, and the challenges and learnings I encountered along the way.

`The goal of this project` was to gain insights into the relationship between traffic violations and weather conditions in San Francisco, and to answer questions such as:

- How does the weather affect the frequency and type of traffic violations?
- Which neighborhoods have the most traffic violations and why?
- What are the common patterns and trends in traffic violations over time?

To achieve this goal, I followed these steps:

1. **`Data collection:`** using Apache Airflow, I created a pipeline that automatically scraped data from the San Francisco government website for traffic violations and from a weather API for hourly weather data per day. The data was collected for a period of one year, from January 1st 2022 to January 31st 2023.
2. `**Data aggregation:**` using Spark RDDs, I joined the traffic violation data with the weather data by matching the date and hour fields. This resulted in a single dataset that contained information about each traffic violation along with the corresponding weather conditions at that time.
3. `**Data filtering**`: using object-oriented programming in Python, I defined several functions that allowed me to filter the data by different criteria, such as date range, violation type, neighborhood, etc. This helped me to focus on the relevant subsets of data for my analysis.
4. **`Data storage:`** using MongoDB, I created a database that stored the aggregated Spark RDD as a collection of documents. I also implemented custom MongoDB functions to perform various operations on the data, such as querying, inserting, updating and deleting records. For example, I created a function that returned the number of documents in the collection, another function that returned the type of weather for a given date and hour, and another function that counted the frequency of different violation types per day.
5. `**Data exploration:**` using SQL, I exported the data from MongoDB into a CSV file that I queries using PySpark SQL. I was able to answer the questions I had about the data and discover some interesting insights. For example, I found out that:
- The most common violation type was speeding, followed by red light violation and stop sign violation.
- The most frequent weather condition was clear sky, followed by partly cloudy and overcast.
- There was a positive correlation between temperature and violation frequency, meaning that more violations occurred on warmer days than on colder days.
- The neighborhood with the most violations was Mission District, followed by South of Market and Tenderloin.
- There was a seasonal pattern in violation frequency, with more violations occurring in summer than in winter.

In conclusion, this project was a great learning experience for me as I got to apply my data engineering skills to a real-world problem. I learned how to use Apache Airflow to automate data collection tasks, how to use Spark to process large-scale data efficiently, how to use MongoDB to store and manipulate unstructured data, and how to use SQL to explore and analyze structured data. I also learned how to combine different tools and techniques to create a comprehensive data pipeline that delivered valuable insights. 
