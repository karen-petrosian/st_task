import pandas as pd

class DataExtractor:
    def __init__(self, df_path, expired_invoices_path):
        self.df = None
        self.expired_invoices = None
        self.df_path = df_path
        self.expired_invoices_path = expired_invoices_path
        self.type_map = {0: 'Material', 1: 'Equipment', 2: 'Service', 3: 'Other'}

    def load_data(self):
        data = pd.read_pickle(self.df_path)
        self.df = pd.DataFrame(data)
        with open(self.expired_invoices_path, "r") as f:
            self.expired_invoices = set(map(str.strip, f.read().split(",")))


    def transform_data(self):
        """
        Transforms the unstructured 'items' column into a flat dataframe.
        """
        if self.df is None:
            raise ValueError("Data not loaded. Please call load_data() first.")

        data = []
        for _, row in self.df.iterrows():
            if not isinstance(row["items"], float):
                for item_data in row["items"]:
                    item = item_data["item"]
                    data.append({
                        "invoice_id": row["id"],
                        "created_on": pd.to_datetime(row["created_on"], errors='coerce'),
                        "invoiceitem_id": item["id"],
                        "invoiceitem_name": item["name"],
                        "type": self.type_map.get(item["type"], "Unknown"),
                        "unit_price": item["unit_price"],
                        "quantity": item_data["quantity"],
                    })

        flat_df = pd.DataFrame(data)
        flat_df["invoice_id"] = pd.to_numeric(flat_df["invoice_id"], errors="coerce").astype("Int64")
        flat_df["unit_price"] = pd.to_numeric(flat_df["unit_price"], errors="coerce")
        flat_df["quantity"] = pd.to_numeric(flat_df["quantity"], errors="coerce")

        flat_df["total_price"] = (flat_df["unit_price"] * flat_df["quantity"]).astype('Int64')
        invoice_totals = flat_df.groupby("invoice_id")["total_price"].transform("sum")
        eps = 1e-8
        flat_df["percentage_in_invoice"] = 100 * flat_df["total_price"] / (invoice_totals + eps)
        flat_df["is_expired"] = flat_df["invoice_id"].isin(self.expired_invoices)

        flat_df[["invoiceitem_name", "type"]] = flat_df[["invoiceitem_name", "type"]].astype(str)

        return flat_df

extractor = DataExtractor("invoices_new.pkl", "expired_invoices.txt")

extractor.load_data()

flat_df = extractor.transform_data()

print(flat_df.dtypes)

print(flat_df)

flat_df_sorted = flat_df.sort_values(by=['invoice_id', 'invoiceitem_id'], ascending=True)

flat_df_sorted.to_csv('sorted_invoices.csv', index=False)
