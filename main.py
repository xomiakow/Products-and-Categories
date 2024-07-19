from pyspark.sql import SparkSession

# Creating SparkSession
spark = SparkSession.builder.appName('test').getOrCreate()

# Setting DataFrames
ProductsDF = spark.read.csv('Products.csv', header=True, inferSchema=True, sep=';')
CategoriesDF = spark.read.csv('Categories.csv', header=True, inferSchema=True, sep=';')
ConnectionsDF = spark.read.csv('ProductsCategories.csv', header=True, inferSchema=True, sep=';')

# Setting temporary views to perform SQL operations
ProductsDF.createOrReplaceTempView('product')
CategoriesDF.createOrReplaceTempView('category')
ConnectionsDF.createOrReplaceTempView('connection')

# Building DataFrame to show
TotalsDF = spark.sql('select Prod_Name, Category_Name from product '
                     'left join connection on product.Prod_ID = connection.Prod_ID '
                     'left join category on category.Category_ID = connection.Category_ID '
                     'order by Prod_Name, Category_Name desc;')

# Output built DataFrame
TotalsDF.withColumnRenamed('Prod_Name', 'Product').withColumnRenamed('Category_Name', 'Category').show()


