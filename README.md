# SparkTransformation
PysparkUsecase1:to  know the following question from orders data with
the schema: order_id, date, customer_id, order_status.
1. Finding Premium Customers
a)Top 10 Premium Customers:
b)Distinct Customers with at least 1 Order:
c)Customers with Maximum 'CLOSED' Orders:
2. Parallelization & Industry Practices
Development Workflow:
Develop logic on "orders"sample data. 
Test functionality on a small dataset before applying to large datasets.
Using parallelize():

3. Checking RDD Partitions
4. CountByValue() vs. map + reduceByKey()
5. Transformations in Spark
Narrow Transformations (No Shuffling): Data stays on the same machine.
 map(), filter(), flatMap()
Wide Transformations (Shuffling): Data is transferred between machines.
Examples: reduceByKey(), groupByKey()
Minimize wide transformations and perform them at the end.

6. Reduce() vs. reduceByKey()

Summary:

Developed Spark RDD-based logic to analyze customer behavior.
Discussed Spark execution environments, parallelization, and transformations.
Compared different Spark functions for performance optimization.
Provided insights on monitoring Spark jobs.
