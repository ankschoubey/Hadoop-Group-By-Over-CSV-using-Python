# Hadoop-Mini-Project

Dataset: [H-1B Visa Petitions 2011-2016](https://www.kaggle.com/nsharan/h-1b-visa/data)

Testing without Hadoop

$ ```head -n 5 h1b_kaggle.csv | python group_by_mapper.py 5 6 | sort -k1,1 | python value_summation_reducer.py```

*Requires python 3+*


**group_by_mapper** 

- Take 1 column as key and other column as value

Command Line arguments for ```group_by_mapper``` are ```key_column``` and ```aggregate_column```

$ ```python run.py group_by 5 6 h1b_kaggle.csv```