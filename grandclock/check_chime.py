"""
Read the wav file and get an array of amplitudes
Take that array and find the peaks.
Chimes are between 1 second to 0.7 seconds apart (at the max).

If chimes are before the correct time, then it is minus. If after it is plus.

Dependencies and assumptions.
The input wav file will be of the 10 minutes around the hour and will not contain multiple hour chimes.

For the chime archive, 20GB of free space, max of 100Mb sound files so assume 200 files can be archived.
Only store ones where the
"""

import os
import sys
import time
from datetime import datetime, timedelta

import gspread
import matplotlib.pyplot as plt
import numpy as np
from oauth2client.service_account import ServiceAccountCredentials
from scipy.io import wavfile
from scipy.signal import find_peaks

credentials_file = f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/credentials.json"
sheet_name = 'GrandfatherClock'
sheet_id = '1cB5zOt3oJHepX2_pdfs69tnRl_HBlReSpetsAoc0jVI'


class WaveAnalysis:
    def __init__(self, file_path):
        self.height = 100
        self.file_path = file_path
        self.fs, amplitude = wavfile.read(file_path)
        self.amplitude = np.absolute(amplitude)
        self.start_time = self.get_start_time()
        self.chime_time = self.get_chime_time()
        self.number_of_chimes = self.get_number_of_chimes()
        self.max_height = np.max(self.amplitude)
        self.min_height = 0
        self.recursion = 0
        self.prominence_max = 400
        self.prominence_min = 100
        self.exit_status = 'Success'

    def get_start_time(self):
        """Datetime object for the start of the sound recording"""
        # Timezone and BST not accounted for. Always gives it as GMT.
        create_time = (os.path.getmtime(self.file_path))
        start_time = create_time - len(self.amplitude) / self.fs
        return datetime.fromtimestamp(start_time)

    def get_chime_time(self):
        """Gets the hour datetime for when the chime should be
        :return datetime object
        """
        actual_time = datetime(year=self.start_time.year, month=self.start_time.month, day=self.start_time.day,
                               hour=self.start_time.hour, minute=0, second=0, microsecond=0)
        if self.start_time.minute > 30:
            actual_time = actual_time + timedelta(hours=1)
        return actual_time

    def get_number_of_chimes(self):
        hour = int(self.chime_time.hour)
        if hour == 0:
            return 12
        elif hour > 12:
            return hour - 12
        else:
            return hour

    @staticmethod
    def _mean_peak_diff(peaks):
        # If 1 chime, there is no valid peak difference so it's just 0.
        if len(peaks) == 1:
            return 0
        peak_diff = [peaks[n] - peaks[n - 1] for n in range(1, len(peaks))]
        mean_diff = (sum(peak_diff) / len(peak_diff))
        return mean_diff

    def _peak_to_times(self, peaks):
        return

    def search_range_for_fit(self, peaks, mean_peak_distance=1.5):
        """

        :param mean_peak_distance: mean distance between each peak in seconds.
        :param peaks: list of peaks to look through to find a set with a small enough mean distance between peaks
        :return: list of lists containing the ranges that fit the given profile
        """
        correct_profile_peaks = []
        mean_peak_distance *= self.fs
        for x in range(0, len(peaks) - self.number_of_chimes + 1):  # For each moving window of sub-peaks.
            sub_range = peaks[x: x + self.number_of_chimes]
            if self._mean_peak_diff(sub_range) < mean_peak_distance:
                correct_profile_peaks.append(sub_range)

        return correct_profile_peaks

    def find_chimes(self):
        """
        Start with broad conditions for finding peaks.
        If there are too many peaks that fit the profile, then narrow the criteria until there is

        :return: list of datetime objects for each chime
        """
        while True:
            self.recursion += 1
            if self.recursion > 10:
                self.exit_status = "Recursion limit reached"
                return None

            peaks, peaks_meta_data = find_peaks(self.amplitude, height=self.height,
                                                distance=int(self.fs / 2),
                                                prominence=[self.prominence_min, self.prominence_max])
            # If correct number of peaks are present, go with that.
            correct_profile_peaks = self.search_range_for_fit(peaks, mean_peak_distance=1.5)
            if len(correct_profile_peaks) == 1:
                return correct_profile_peaks[0]  # Correct peaks in there.

            elif len(correct_profile_peaks) == 0:
                # not captured any peaks so broaden the search, drop height, drop prom min and increase prom max
                # Height is too high
                self.max_height = self.height
                self.height = (self.max_height - self.min_height) // 2

                # Prominence
                self.prominence_min -= self.prominence_min // 4
                self.prominence_max += self.prominence_max // 4

            elif len(correct_profile_peaks) > 1:
                # Captured too many peaks so narrow the criteria.
                self.min_height = self.height
                self.height = (self.max_height - self.min_height) * 2

                self.prominence_min += self.prominence_min // 4
                self.prominence_max -= self.prominence_max // 4

    def find_drift(self):
        """Expects just the chime times, no noise
            :return drift (seconds, negative is too fast), the aimed for time as datetime object.
        """
        peaks = self.find_chimes()
        if peaks is None:
            return None, self.chime_time

        chimes = [peak / self.fs for peak in peaks]  # Convert raw peaks to relative seconds (0 - 10*60)
        chimes = [self.start_time + timedelta(seconds=chime) for chime in
                  chimes]  # Convert to actual time (10:55 - 11:05)

        first_chime = chimes[0]

        if first_chime.minute > 30:  # The it is before the chime so add an hour to first chime hour.
            drift_direction = -1
        else:
            drift_direction = 1

        drift = abs(self.chime_time - first_chime)
        drift = drift.seconds
        drift = drift * drift_direction

        return drift, self.chime_time

    def show_waveform(self, peaks=list):
        """
        Shows waveform for self.amplitude
        :param: peaks, list of peaks to highlight.
        :return: None
        """
        if peaks is None:
            peaks = []
        data = self.amplitude
        x_axis = range(0, len(data))
        x_axis = [x / self.fs for x in x_axis]
        plt.plot(x_axis, data)
        plt.axhline(self.height)
        for p in peaks:
            plt.axvline(p / self.fs, color="red", alpha=0.2)
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

    def send_it(self, func, limit, *args, **kwargs):
        """
        Wrapper function for sending data to sheets, this will handle the try and except and the iteration
        written as: self.send_it(self.sheet.method, limit=5, arg1=1, arg2=2, )
        :param func:
        :param limit:
        :param args:
        :param kwargs:
        :return:
        """
        counter = 0
        if counter > limit:
            return False
        counter += 1
        try:
            result = func(*args, **kwargs)
            time.sleep(1.1)
            return result
        except gspread.exceptions.APIError as e:
            if (e.response.json())['error']['code'] == 429:
                time.sleep(501)
                self.send_it(func, limit, *args, **kwargs)
            else:
                print(e)
                return False
        except Exception as e:
            print(e)
            return False

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
                if item is None:
                    item = "=na()"

                self.send_it(self.sheet.update_cell, limit=5, row=next_free_row, col=column, value=item)
                column += 1
            next_free_row += 1

    def _reverse_values(self, headers_to_skip=1):
        """
        Yields data going up the sheet:
        [[col 1, col 2], [col 1, col 2], ... for each row]
        :return: dict{index, [values]
        """
        result = self.send_it(self.sheet.get_all_values, limit=5)
        result = result[headers_to_skip:]
        index = [*range(2, len(result) + 2)]
        result.reverse()
        index.reverse()
        data = dict(zip(index, result))
        return data

    def insert_na(self):
        """Insert missing na rows where data has been skipped for some reason
        find the diff in time between each row

        insert_row - index values is the row number you want to add in.

        Move up the column. Stops the above rows from moving.
        """

        values = self._reverse_values()
        for i, t in values.items():
            if i == 2:  # index 2 is the top item so n-1 time is not possible.
                continue
            t = datetime.strptime(t[0], '%Y-%m-%d %H:%M:%S')

            n_1_time = datetime.strptime(values[i - 1][0], '%Y-%m-%d %H:%M:%S')

            diff = int((t - n_1_time).seconds / 3600)
            for r in range(1, diff):  # Skips when diff if 1.
                print(i, diff)
                self.send_it(
                    self.sheet.insert_row, limit=5,
                    values=[(t - timedelta(hours=r)).strftime('%Y-%m-%d %H:%M:%S'), "=na()"],
                    index=i,
                    value_input_option="USER_ENTERED")

    def remove_duplicates(self):
        values = self._reverse_values()
        for index, items in values.items():
            if items[0] == '':
                self.send_it(self.sheet.delete_row, limit=5, index=index)

    def remove_blanks(self):
        values = self._reverse_values()
        for index, times in values.items():
            try:
                datetime.strptime(times[0], '%Y-%m-%d %H:%M:%S')
            except Exception:
                self.send_it(self.sheet.delete_row, limit=5, index=index)


class ArchiveManager:
    def __init__(self, archive=os.path.abspath(f'{os.path.expanduser("~")}/archive/')):
        self.archive = archive

    def get_archive_files(self):
        files = os.listdir(self.archive)
        files = [f for f in files if f.endswith('.wav')]
        files.sort()
        return files

    def remove_excess_files(self):
        files = self.get_archive_files()

        if len(files) > 168:
            os.remove(f'{self.archive}/{files[0]}')
            self.remove_excess_files()  # Calls itself recursively to ensure that only 168 files remain.

    def find_and_update_from_archive(self):
        post_to_sheets = PostToSheets(sheet_name, sheet_id)
        values = post_to_sheets.sheet.get_all_values()

        for file in self.get_archive_files():
            t = datetime.strptime(file.split(".")[0], '%Y-%m-%d_%H').strftime('%Y-%m-%d %H:%M:%S')
            if [t, "#N/A"] in values:
                index = values.index([t, "#N/A"]) + 1  # +1 as sheet starts at 1, not 0.
                drift = WaveAnalysis(f'{self.archive}{file}').find_drift()[0]
                if drift is not None:
                    post_to_sheets.send_it(post_to_sheets.sheet.update_cell, limit=5, row=index, col=2, value=drift)
                    os.remove(f'{self.archive}{file}')
            else:
                os.remove(f'{self.archive}{file}')

    def save_data_to_archive(self, archive_file=f'{os.path.expanduser("~")}/clock_archive.txt'):
        """Saves new data to the json archive"""
        post_to_sheets = PostToSheets(sheet_name, sheet_id)
        values = post_to_sheets.sheet.get_all_values()[1:]  # Skip header.
        if os.path.exists(archive_file) is False:
            with open(archive_file, 'w') as f:
                f.write("Aimed for time, Drift (seconds)\n")
                values_as_string = '\n'.join([",".join(v) for v in values])
                f.writelines(values_as_string)
        else:
            with open(archive_file, 'r') as f:
                lines = f.readlines()
                last_item = lines[-1].split(',')

            with open(archive_file, 'a') as f:
                for item in values[values.index(last_item)+1:]:
                    f.write(f'{",".join(item)}\n')

    def adjust_sheet_length(self):
        """Keeps sheet to only 30 days worth of data"""
        post_to_sheets = PostToSheets(sheet_name, sheet_id)
        values = post_to_sheets.sheet.get_all_values()[1:]  # Skip header.
        rows_to_remove = len(values) - (30 * 24)
        for i in range(2, rows_to_remove + 2):  # +2 for header and index start at 1.
            post_to_sheets.send_it(post_to_sheets.sheet.delete_row, limit=2, index=2)


def main():
    """
    Only keeps files that fail to upload real data.
    :return:
    """
    post = PostToSheets('GrandfatherClock', '1cB5zOt3oJHepX2_pdfs69tnRl_HBlReSpetsAoc0jVI')
    post.remove_blanks()
    post.remove_duplicates()
    post.insert_na()

    archive_manager = ArchiveManager()
    archive_manager.find_and_update_from_archive()
    archive_manager.remove_excess_files()

    if len(sys.argv) > 1:
        wav_file = os.path.abspath(f'{os.path.expanduser("~")}/{sys.argv[1]}')
        print(wav_file)
    else:
        wav_file = os.path.abspath(f'{os.path.expanduser("~")}/chime.wav')
    # fs = 44100

    # Needs to 1. Find the latest value and update the sheet and the archive.
    try:
        drift, actual_time = WaveAnalysis(wav_file).find_drift()
        actual_time = actual_time.strftime('%Y-%m-%d %H:%M:%S.%f')
        PostToSheets('GrandfatherClock', '1cB5zOt3oJHepX2_pdfs69tnRl_HBlReSpetsAoc0jVI').post_data(
            [[actual_time, drift]])
    except Exception as error:
        print(f"Error at {datetime.now()}: {error}")

    archive_manager.save_data_to_archive()
    archive_manager.adjust_sheet_length()


# todo - remove clearly incorrect data points, more than 3 SDs from past 24 hours worth of data away from mean ish.


if __name__ == '__main__':
    main()

