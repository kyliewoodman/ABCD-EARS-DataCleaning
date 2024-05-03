#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd

# Load the data
data = pd.read_csv('/srv/lab/fmri/abcd/java_name_conversion.csv')  # Adjust the file path and format (e.g., use read_excel for .xlsx files)

# Conversion function
def convert_to_app_name(java_name):
    parts = java_name.split('.')
    app_name = parts[-1]
    return ''.join([' '+char if char.isupper() else char for char in app_name]).lstrip()

# Apply the conversion
data['App Name'] = data['id_app'].apply(convert_to_app_name)

# Save the updated DataFrame
data.to_csv('updated_file_path.csv', index=False)


# In[2]:


data.head()


# In[4]:


def convert_java_name_to_app_name(java_name):
    # Take the last component after the last dot
    last_component = java_name.split('.')[-1]
    
    # Replace underscores with spaces and handle camelCase
    spaced_name = ''.join([' ' + char if char.isupper() else char for char in last_component]).replace('_', ' ').strip()
    
    # Capitalize the first letter of each word
    app_name = ' '.join(word.capitalize() for word in spaced_name.split())
    
    return app_name

# List of Java package names

data = pd.read_csv('/srv/lab/fmri/abcd/java_name_conversion.csv')
java_names = data['id_app']

# Convert each Java name and print the app name
app_names = [convert_java_name_to_app_name(name) for name in java_names]
for java_name, app_name in zip(java_names, app_names):
    print(f"Java Name: {java_name} -> App Name: {app_name}")


# In[ ]:




