# Homework

The [linked colab notebook](https://colab.research.google.com/drive/1Te-AT0lfh0GpChg1Rbd0ByEKOHYtWXfm#scrollTo=wLF4iXf-NR7t&forceEdit=true&sandboxMode=true) offers a few exercises to practice what you learned today.


#### Question 1: What is the sum of the outputs of the generator for limit = 5?
- **A**: 10.23433234744176
- **B**: 7.892332347441762
- **C**: 8.382332347441762
- **D**: 9.123332347441762

### Sol: C or 8.382332347441762

#### Question 2: What is the 13th number yielded by the generator?
- **A**: 4.236551275463989
- **B**: 3.605551275463989
- **C**: 2.345551275463989
- **D**: 5.678551275463989

### Sol: B or 3.605551275463989
```
# for both Q1 and Q2
def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1
        
limit = 5
generator = square_root_generator(limit)
sum_of_outputs = sum(generator)
print(sum_of_outputs)
limit = 13
generator = square_root_generator(limit)
for ex in range(12):  # Iterate 12 times to reach the 13th number
    next(generator)
thirteenth_number = next(generator)
print(thirteenth_number)
```
#### Question 3: Append the 2 generators. After correctly appending the data, calculate the sum of all ages of people.
- **A**: 353
- **B**: 365
- **C**: 378
- **D**: 390

### Sol: A or 353
```
import dlt
import duckdb

# Define the connection to load to.
generators_pipeline = dlt.pipeline(destination='duckdb', dataset_name='generators')

# Define your generators
def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}

def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}

# Load the first generator to the "people_data" table
info_people_1 = generators_pipeline.run(people_1(), table_name="people_data", write_disposition="replace")

# Append the second generator to the same table
info_people_2 = generators_pipeline.run(people_2(), table_name="people_data", write_disposition="append")

# Show outcome
print(info_people_1)
print(info_people_2)

# Connect to DuckDB
conn = duckdb.connect(f"{generators_pipeline.pipeline_name}.duckdb")

# Set the search path
conn.sql(f"SET search_path = '{generators_pipeline.dataset_name}'")

# Calculate the sum of ages for all people
sum_of_ages = conn.execute("SELECT SUM(Age) FROM people_data").fetchone()[0]
print("Sum of ages (Total):", sum_of_ages)

# Display data from "people_data" table
print("\n\n\n people_data table below:")
people_data = conn.sql("SELECT * FROM people_data").df()
display(people_data)
```
#### Question 4: Merge the 2 generators using the ID column. Calculate the sum of ages of all the people loaded as described above.
- **A**: 215
- **B**: 266
- **C**: 241
- **D**: 258

```
import dlt
import duckdb

# Define the connection to load to.
generators_pipeline = dlt.pipeline(destination='duckdb', dataset_name='merged_generators')

# Define your generators
def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}

def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}

# Load the first generator to the "people_data" table with a primary key
info_people_1 = generators_pipeline.run(people_1(), table_name="people_data", write_disposition="replace", primary_key="ID")

# Load the second generator and merge it into the existing table
info_people_2 = generators_pipeline.run(people_2(), table_name="people_data", write_disposition="merge", primary_key="ID")

# Show outcome
print(info_people_1)
print(info_people_2)

# Connect to DuckDB
conn = duckdb.connect(f"{generators_pipeline.pipeline_name}.duckdb")

# Set the search path
conn.sql(f"SET search_path = '{generators_pipeline.dataset_name}'")

# Calculate the sum of ages for all people
sum_of_ages = conn.execute("SELECT SUM(Age) FROM people_data").fetchone()[0]
print("Sum of ages (Total):", sum_of_ages)

# Display data from "people_data" table
print("\n\n\n people_data table below:")
people_data = conn.sql("SELECT * FROM people_data").df()
display(people_data)
```
### Sol: B or 266
#### All_soltion is incuding in homework_notebook in this folder
