"""
Keep files where data failed to upload to the google sheet
"""
import datetime
import os
import grandclock.check_chime as chime

archive = os.path.abspath(f'{os.path.expanduser("~")}/archive/')

def get_archive_files():
    files = os.listdir(archive)
    files = [f for f in files if f.endswith('.wav')]
    files.sort()
    return files


def remove_excess_files():
    files = get_archive_files()

    if len(files) > 168:
        os.remove(f'{archive}/{files[0]}')
        remove_excess_files()  # Calls itself recursively to ensure that only 168 files remain.


def main():
    """
    For each item in archive, try it and see if it works. Remove if successful. Keep otherwise.
    :return:
    """
    files = get_archive_files()

    for wav_file in files:
        try:
            drift, actual_time = chime.WaveAnalysis(wav_file, height=200).find_drift()
            actual_time = actual_time.strftime('%Y-%m-%d %H:%M:%S.%f')
            chime.PostToSheets('GrandfatherClock', '1cB5zOt3oJHepX2_pdfs69tnRl_HBlReSpetsAoc0jVI').post_data(
                [[actual_time, drift]])
            if drift == "=na()":
                os.renames(wav_file, os.path.abspath(
                    f'{os.path.expanduser("~")}/archive/{actual_time.strftime("%Y-%m-%d_%H")}.wav'))
        except Exception as error:
            print(f"Error at {datetime.now()}: {error}")
            wav_time = chime.WaveAnalysis(wav_file).chime_time()
            os.renames(wav_file,
                       os.path.abspath(f'{os.path.expanduser("~")}/archive/{wav_time.strftime("%Y-%m-%d_%H")}.wav'))


if __name__ == '__main__':
    main()

