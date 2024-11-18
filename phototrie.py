import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import os
from PIL import Image
from io import BytesIO
import hashlib
import time
from tqdm import tqdm
import sys
import gc
from pathlib import Path

def get_workspace_folder():
    return os.getcwd()

class ImageDownloader:
    def __init__(self, workspace_path):
        self.workspace_path = workspace_path
        self.images_folder = os.path.join(workspace_path, "pokemon_images")
        self.high_value_folder = os.path.join(self.images_folder, "high_value")
        self.low_value_folder = os.path.join(self.images_folder, "low_value")
        self.processed_urls = set()
        self.max_retries = 3
        self.session = self.setup_session()
        self.setup_folders()

    def setup_folders(self):
        os.makedirs(self.high_value_folder, exist_ok=True)
        os.makedirs(self.low_value_folder, exist_ok=True)

    def setup_session(self):
        session = requests.Session()
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        session.mount('http://', HTTPAdapter(max_retries=retries))
        session.mount('https://', HTTPAdapter(max_retries=retries))
        return session

    def get_target_folder(self, value_type):
        return self.high_value_folder if value_type == 'high' else self.low_value_folder

    def verify_image_saved(self, filepath):
        try:
            if not os.path.exists(filepath):
                return False
            with Image.open(filepath) as img:
                img.verify()
            if os.path.getsize(filepath) < 1024:
                return False
            return True
        except Exception:
            return False

    def process_image(self, image):
        target_size = (224, 224)
        image = image.resize(target_size, Image.LANCZOS)
        
        output = BytesIO()
        image.save(output, format='JPEG', quality=85, optimize=True)
        processed_image = Image.open(output)
        
        return processed_image

    def save_image_with_retry(self, processed_image, filepath):
        for attempt in range(self.max_retries):
            try:
                processed_image.save(filepath, 'JPEG', quality=85)
                if self.verify_image_saved(filepath):
                    return True
                else:
                    time.sleep(1)
            except Exception:
                time.sleep(1)
        
        return False

    def download_image(self, url, index, value_type):
        if url in self.processed_urls:
            return None

        try:
            filename = f"pokemon_{value_type}_{index}_{hashlib.md5(url.encode()).hexdigest()[:8]}.jpg"
            target_folder = self.get_target_folder(value_type)
            filepath = os.path.join(target_folder, filename)

            if os.path.exists(filepath) and self.verify_image_saved(filepath):
                self.processed_urls.add(url)
                return filepath

            response = self.session.get(url, timeout=10)
            if response.status_code != 200:
                return None

            try:
                image = Image.open(BytesIO(response.content))
            except Exception:
                return None
            
            if image.mode != 'RGB':
                image = image.convert('RGB')
            
            processed_image = self.process_image(image)
            
            if self.save_image_with_retry(processed_image, filepath):
                self.processed_urls.add(url)
                return filepath
            else:
                return None

        except Exception:
            return None

        finally:
            if 'image' in locals():
                image.close()
            if 'processed_image' in locals():
                processed_image.close()
            gc.collect()

    def reset_session(self):
        self.session.close()
        self.session = self.setup_session()

def process_csv_files():
    try:
        # Demande à l'utilisateur les bornes de traitement
        start_index = int(input("Entrez l'indice de début du traitement: "))
        end_index = int(input("Entrez l'indice de fin du traitement: "))
        
        workspace = get_workspace_folder()
        high_value_path = os.path.join(workspace, "vinted_data_high.csv")
        low_value_path = os.path.join(workspace, "vinted_data_low.csv")

        if not os.path.exists(high_value_path) or not os.path.exists(low_value_path):
            raise FileNotFoundError("Les fichiers CSV n'ont pas été trouvés dans le dossier de travail")

        downloader = ImageDownloader(workspace)

        df_high = pd.read_csv(high_value_path)
        df_low = pd.read_csv(low_value_path)

        dfs = [
            ('high', df_high),
            ('low', df_low)
        ]

        all_data = []
        failed_downloads = []
        batch_size = 90
        global_index = start_index

        while global_index < end_index:
            print(f"\nDébut du traitement des images à partir de l'index global: {global_index}")
            
            for df_type, df in dfs:
                for index, row in tqdm(df.iloc[global_index:min(global_index + batch_size, end_index)].iterrows(), total=min(batch_size, end_index - global_index)):
                    local_path = downloader.download_image(row['Photo'], index, df_type)
                    
                    if local_path:
                        row_data = row.to_dict()
                        row_data['local_path'] = os.path.relpath(local_path, workspace)
                        row_data['value_type'] = df_type
                        all_data.append(row_data)
                    else:
                        failed_downloads.append({
                            'url': row['Photo'],
                            'type': df_type,
                            'index': index
                        })
                    
                    time.sleep(0.1)
            
            global_index += batch_size
            downloader.reset_session()
            gc.collect()

        final_df = pd.DataFrame(all_data)
        output_csv = os.path.join(workspace, 'pokemon_cards_processed.csv')
        final_df.to_csv(output_csv, index=False)

        if failed_downloads:
            failed_df = pd.DataFrame(failed_downloads)
            failed_csv = os.path.join(workspace, 'failed_downloads.csv')
            failed_df.to_csv(failed_csv, index=False)

        print("\nTraitement terminé avec succès!")

    except Exception as e:
        print(f"\nErreur critique: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        process_csv_files()
    except Exception as e:
        sys.exit(1)
