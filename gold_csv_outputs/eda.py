import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load CSV files
customer_df = pd.read_csv("customer_agg.csv")   # use full path if needed
product_df = pd.read_csv("product_agg.csv")     # use full path if needed

# Basic info
print("Customer Data Overview:")
print(customer_df.info())
print(customer_df.describe())
print(customer_df.isnull().sum())

print("\nProduct Data Overview:")
print(product_df.info())
print(product_df.describe())
print(product_df.isnull().sum())

# Shape of datasets
print(f"Customer Data Shape: {customer_df.shape}")
print(f"Product Data Shape: {product_df.shape}")

# Correlation heatmap for numerical columns
plt.figure(figsize=(6, 4))
sns.heatmap(customer_df.corr(numeric_only=True), annot=True, cmap="coolwarm")
plt.title("Correlation Heatmap - Customer Data")
plt.show()

plt.figure(figsize=(6, 4))
sns.heatmap(product_df.corr(numeric_only=True), annot=True, cmap="coolwarm")
plt.title("Correlation Heatmap - Product Data")
plt.show()

# Distribution plots for numeric columns
numeric_cols_cust = customer_df.select_dtypes(include='number').columns
for col in numeric_cols_cust:
    plt.figure()
    sns.histplot(customer_df[col], kde=True)
    plt.title(f"Distribution of {col}")
    plt.show()

numeric_cols_prod = product_df.select_dtypes(include='number').columns
for col in numeric_cols_prod:
    plt.figure()
    sns.histplot(product_df[col], kde=True)
    plt.title(f"Distribution of {col}")
    plt.show()

# Boxplots to find outliers
for col in numeric_cols_cust:
    plt.figure()
    sns.boxplot(x=customer_df[col])
    plt.title(f"Outlier Detection - {col}")
    plt.show()

for col in numeric_cols_prod:
    plt.figure()
    sns.boxplot(x=product_df[col])
    plt.title(f"Outlier Detection - {col}")
    plt.show()
