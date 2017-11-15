# tennis

This repository is devoted to exploring machine learning ideas on professional tennis data.  The machine learning ideas are mostly those covered in Andrew Ng's "Machine Learning" course (Coursera).  The goals of this project are threefold:

1.  to cement the course ideas by extending them to a new context
2.  to gain experience implementing machine learning algorithms in Python
3.  to gain experience with the Python-MySQL interface.  

The data for this project comes from two sources:  raw player data is from https://github.com/JeffSackmann/tennis_atp, and public betting odds data is from http://www.tennis-data.co.uk/alldata.  

There are four Python Jupyter Notebooks in this repository, each devoted to a different task:

1.  setup_mysql.ipynb:  provides code to port the CSV file data to a MySQL database.
2.  merge_datasets.ipynb:  provides code to establish a one-to-one relation between matches from the two data sources.  
3.  extract_features.ipynb:  provides code to extract learning-worthy features from the player data
4.  tennis_predictions.ipynb:  use the features to try to predict match outcomes.

Many of the underlying ideas come from Sipko, "Machine learning for the predicition of professional tennis matches", 2015.  However, I have access to a slightly different set of data, and use somewhat different features.    
