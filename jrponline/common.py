import os

def fetch_or_create_file(file_path, file_mode = "a"):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    return open(file_path, file_mode, newline="")

def export_datafrane_insights(file, df):
    grouped = df.groupby(['Category Name', 'Product Category'])
    fw = fetch_or_create_file(file, "w")

    fw.write(f"Total Rows: {len(df)}\n\n")    
    fw.write(str(grouped.size()) + '\n')
    # fw.write(str(grouped.describe()))
    # print(1/0)