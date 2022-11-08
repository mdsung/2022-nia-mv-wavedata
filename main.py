from pathlib import Path

import pandas as pd

DATA_PATH = Path("/home/ubuntu/2022-nia-mv/아주대/csv")
OUTPUT_DATA_PATH = Path("/home/ubuntu/2022-nia-mv/아주대/processed")

for f in DATA_PATH.glob("**/2-*"):
    output_path = Path(str(f).replace('csv', 'processed'))
    output_path.mkdir(parents=True, exist_ok=True)
    for file1, file2 in zip(Path(f, 'AWP').glob("*.csv"), Path(f, 'AWF').glob("*.csv")):
        output_file_path = Path(output_path, file1.name)
        output = pd.merge(pd.read_csv(file1), pd.read_csv(file2))
        output.to_csv(output_file_path, index = False)
