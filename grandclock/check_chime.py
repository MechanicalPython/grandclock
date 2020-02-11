"""
Read the wav file and get an array of amplitudes
Take that array and find the peaks.
Chimes are between 1 second to 0.7 seconds apart (at the max).

Dependencies and assumptions.
The input wav file will be of the 10 minutes around the hour and will not contain multiple hour chimes.
"""

import os
import re
import time
from datetime import datetime, timedelta

import gspread
import matplotlib.pyplot as plt
import numpy as np
from oauth2client.service_account import ServiceAccountCredentials
from scipy.io import wavfile
from scipy.signal import find_peaks

credentials_file = f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/credentials.json"

# fs = 44100
wav_file = '/Users/Matt/4pm.wav'
fs, data = wavfile.read(wav_file)


def get_chime_times(data):
    """Finds the peaks (1000 times base levels, 1 second around each peak)
    create_time is when the file was created -> when the recording stops.
    :return: list of times (%Y-%m-%d %H:%M:%S) for chimes
    """
    peaks = find_peaks(data, height=150, distance=fs / 2, prominence=1)

    create_time = (os.path.getctime(wav_file))
    start_time = create_time - len(data) / fs
    times = [datetime.utcfromtimestamp(start_time + peak / fs) for peak in peaks[0]]

    # Remove false data.
    aim_hour = datetime.utcfromtimestamp(start_time).hour + 1
    if aim_hour > 12:
        aim_hour = aim_hour - 12

    e = aim_hour
    for s in range(0, len(times) - (aim_hour - 1)):
        chunk = times[s: e]
        dif = [j-i for i, j in zip(chunk[:-1], chunk[1:])]
        if np.mean(dif) < timedelta(seconds=2):
            # chunk = [time.strftime('%Y-%m-%d %H:%M:%S.%f') for time in chunk]
            return chunk
        e += 1


def extract_drift(chime_times):
    """Expects just the chime times, no noise"""
    first_chime = chime_times[0]

    if first_chime.minute > 30:
        actual_time = first_chime.hour + 1
    else:
        actual_time = first_chime.hour
    actual_time = timedelta(hours=actual_time)
    first_chime = timedelta(hours=first_chime.hour, minutes=first_chime.minute, seconds=first_chime.second,
                            microseconds=first_chime.microsecond)

    drift = first_chime - actual_time
    return drift


def compress_waveform(array, compression_rate):
    """Compress x number of items into a single mean value
    Not useful
    """
    compression_rate = int(compression_rate)

    def chunks(l, n):
        for i in range(0, len(l), n):
            yield l[i:i + n]

    compressed_array = [np.mean(chunk) for chunk in chunks(array, compression_rate)]
    return compressed_array


def show_waveform(data):
    x_axis = range(0, len(data))
    x_axis = [x / fs for x in x_axis]
    plt.plot(x_axis, data)
    plt.ylabel("Amplitude")
    plt.xlabel("Time (seconds)")
    plt.title("Waveform")
    plt.show()


class PostToSheets:
    """
    From data.pkl, post the data to google sheets, in chronological order
    """

    def __init__(self, sheet_name, SHEET_ID):
        self.SCOPE = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
        self.SHEET_ID = SHEET_ID
        self.creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, self.SCOPE)
        self.client = gspread.authorize(self.creds)
        self.sheet = self.client.open(sheet_name).sheet1

    def post_data(self, data):
        """
        data shape must be row, column
        a  b  c
        d  e  f
        should be: [[a. b, c], [d, e, f]]
        :param data:
        :return:
        """
        # Check for enough room in spreadsheet
        num_rows = len(data)
        max_rows = self.sheet.row_count
        next_free_row = len(self.sheet.col_values(1)) + 1
        if num_rows + next_free_row > max_rows:
            self.sheet.add_rows(num_rows + 10)  # Adds more rows if needed.

        for row in data:
            column = 1
            for item in row:
                try:
                    self.sheet.update_cell(next_free_row, column, item)
                    time.sleep(1)
                except Exception as e:
                    if re.search('"code": 429', str(e)):
                        time.sleep(100)
                column += 1
                time.sleep(1.1)  # To avoid the 100 requests per 100 seconds limit
            next_free_row += 1


if __name__ == '__main__':
    drift = extract_drift(get_chime_times(data))
    PostToSheets('GrandfatherClock', '1cB5zOt3oJHepX2_pdfs69tnRl_HBlReSpetsAoc0jVI').post_data([[drift]])

