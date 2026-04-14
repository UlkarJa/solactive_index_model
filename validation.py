import pandas as pd

calc = pd.read_csv("export.csv")
ref = pd.read_csv("data_sources/index_level_results_rounded.csv")

# convert dates
calc["Date"] = pd.to_datetime(calc["Date"], dayfirst=True)
ref["Date"] = pd.to_datetime(ref["Date"], dayfirst=True)

# merge both tables
comparison = pd.merge(calc, ref, on="Date", how="inner")

# Add new columns for reconciliation
comparison.columns = ["Date", "Calculated", "Reference"]
comparison["Difference"] = comparison["Calculated"] - comparison["Reference"]

print(comparison)

# deviation check
print("\nMax difference:", comparison["Difference"].abs().max())