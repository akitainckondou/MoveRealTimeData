import os
import shutil
import subprocess
from configparser import RawConfigParser
from datetime import datetime, timedelta
from pathlib import Path


class MoveRealTimeData:
    source = None
    destination = None
    days = None
    cmd = None
    s3_copy = None
    threshold = None
    archive_path = None
    retention_period = None

    def __init__(self):
        config_parser = RawConfigParser()
        config_file_path = r'config.ini'
        config_parser.read(config_file_path, 'UTF-8')

        self.source = config_parser.get('CONFIG', 'source')
        self.destination = config_parser.get('CONFIG', 'destination')
        self.days = int(config_parser.get('CONFIG', 'days'))
        self.retention_period = int(config_parser.get('CONFIG', 'retention_period'))
        # この日以前のデータを移動
        self.threshold = (datetime.now() - timedelta(days=self.days)).replace(hour=0, minute=0, second=0, microsecond=0)
        suffix = f"{(self.threshold - timedelta(days=1)).strftime('%Y%m%d')}_{datetime.now().strftime('%Y%m%d%H%M%S')}"

        zip_command = config_parser.get('CONFIG', 'cmd')
        self.archive_path = config_parser.get('CONFIG', 'archive_path')
        archive_file = os.path.join(self.archive_path, f"realtime_{suffix}.7z")
        aws_bucket = config_parser.get('CONFIG', 'aws_bucket')

        self.cmd = f"{zip_command} {archive_file} {self.destination}"
        self.s3_copy = f"aws s3 cp {archive_file} {aws_bucket}"

    def process(self):
        # 対象フォルダ内に存在する営業所毎のフォルダ一覧
        directories = os.listdir(self.source)

        for directory in directories:
            self.__move_process(directory)
        try:
            # 7z形式で圧縮
            subprocess.call(self.cmd)
            # 圧縮済みファイルをS3へアップロード
            subprocess.call(self.s3_copy)
        except Exception as e:
            print(f"Exception: {e.args}")
            return

        self.__remove_directories()
        self.__remove_archived_file()

    def __move_process(self, directory):
        """
        対象ディレクトリ内のファイルを移動
        :param directory:
        """
        source_dir = os.path.join(self.source, directory)
        if Path.is_file(Path(source_dir)):
            return

        # 各営業所フォルダ内に存在するCSVファイル
        files = os.listdir(source_dir)
        destination_dir = os.path.join(self.destination, directory)
        # 移動先に同名のフォルダがなければ作成
        if not os.path.exists(destination_dir):
            os.mkdir(destination_dir)
        for file in files:
            self.__move_file(file, source_dir, destination_dir)
        print(f"{directory}:ファイル移動処理完了")

    def __move_file(self, file, source_dir, destination_dir):
        created = datetime.fromtimestamp(os.path.getmtime(os.path.join(source_dir, file)))
        # 閾値より最終更新日が古いファイルを移動
        if created < self.threshold:
            shutil.move(os.path.join(source_dir, file), os.path.join(destination_dir, file))

    def __remove_directories(self):
        """
        圧縮処理後にディレクトリを空にする
        """
        shutil.rmtree(self.destination)
        os.mkdir(self.destination)

    def __remove_archived_file(self):
        """
        古い圧縮ファイルの削除
        """
        files = os.listdir(self.archive_path)
        for file in files:
            full_path = os.path.join(self.archive_path, file)
            created = datetime.fromtimestamp(os.path.getmtime(full_path))
            if created < (datetime.now() - timedelta(days=self.retention_period)):
                # S3アップロード後は削除
                os.unlink(full_path)


def main():
    method = MoveRealTimeData()
    method.process()
    print("処理終了しました")


if __name__ == '__main__':
    main()
