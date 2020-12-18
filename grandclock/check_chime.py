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
from oauth2client.service_account import ServiceAccountCredentials
from scipy.io import wavfile
from scipy.signal import find_peaks

credentials_file = f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/credentials.json"


class WaveAnalysis:
    def __init__(self, file_path, height=200):
        self.height = height
        self.file_path = file_path
        self.fs, self.amplitude = wavfile.read(file_path)
        self.start_time = self.get_start_time()
        self.chime_time = self.get_chime_time()
        self.number_of_chimes = self.get_number_of_chimes()
        self.too_high = None
        self.too_low = None
        self.recursion = 0

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

    def find_chimes(self):
        """Finds the peaks (200 times base levels, 1 second around each peak)
        create_time is when the file was created -> when the recording stops.

        Recursive search algorithm.
        Start number = x
        If too high, n+1 = x/2
        If too low, new guess = x*2
        if n+1

        :return: list of datetime objects for each chime
        """
        while True:
            self.recursion += 1
            if self.recursion > 10:
                return None
            peaks, peaks_meta_data = find_peaks(self.amplitude, height=self.height, distance=self.fs / 2, prominence=1)
            peaks = [peak / self.fs for peak in peaks]

            # If correct number of peaks are present, go with that.
            if len(peaks) == self.number_of_chimes and self._mean_peak_diff(peaks) < 1.5:  # Correct peaks
                return [self.start_time + timedelta(seconds=peak) for peak in peaks]

            # -- Incorrect number of peaks are present.

            # Too many peaks
            elif len(peaks) > self.number_of_chimes:  # too many peaks, height is too low -> increase height
                all_sub_peaks = []

                # If there are too many peaks, the correct peaks could be there, so for a moving window of peaks
                # Look at each one and see if they fit the desired profile
                # E.g. peaks = [10, 500,501,502,503] for 4pm.
                # Look at [10, 500, 501, 502], incorrect, peaks too far apart.
                # Look at [500, 501, 502, 503] correct, peaks fit the profile.
                for x in range(0, len(peaks) - self.number_of_chimes + 1):  # For each moving window of sub-peaks.
                    sub_peaks = peaks[x: x + self.number_of_chimes]
                    if self._mean_peak_diff(sub_peaks) < 1.5:
                        all_sub_peaks.append(sub_peaks)

                # If there is just one sub_peak range that fits, use it.
                if len(all_sub_peaks) == 1:
                    peaks = all_sub_peaks[0]
                    return [self.start_time + timedelta(seconds=peak) for peak in peaks]  # Correct peaks in there.
                else:
                    # height is too low -> too many peaks -> increase threshold height.
                    # print('too low', self.height)
                    self.too_low = self.height
                    if self.too_high is None:
                        self.height = self.height * 2
                    else:
                        self.height = int(((self.too_high - self.too_low) / 2) + self.too_low)

                    self.find_chimes()

            # Too few peaks
            elif len(peaks) < self.number_of_chimes:  # too few peaks -> reduce height
                # print('too high', self.height)
                self.too_high = self.height
                if self.too_low is None:
                    self.height = self.height / 2
                else:
                    self.height = int(((self.too_high - self.too_low) / 2) + self.too_low)
                self.find_chimes()

            # Reached when number of peaks = number of chimes but the peaks are not of the correct profile.
            else:
                return None

    def find_drift(self):
        """Expects just the chime times, no noise
            :return drift (seconds, negative is too fast), the aimed for time as datetime object.
        """
        chimes = self.find_chimes()
        if chimes is None:
            return None, self.chime_time

        first_chime = chimes[0]

        if first_chime.minute > 30:  # The it is before the chime so add an hour to first chime hour.
            drift_direction = -1
        else:
            drift_direction = 1

        drift = abs(self.chime_time - first_chime)
        drift = drift.seconds
        drift = drift * drift_direction

        return drift, self.chime_time

    def show_waveform(self):
        data = self.amplitude
        x_axis = range(0, len(data))
        x_axis = [x / self.fs for x in x_axis]
        plt.plot(x_axis, data)
        plt.axhline(self.height)
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
                if item is None:
                    item = "=na()"

                def send_it(tries=0):
                    try:
                        self.sheet.update_cell(next_free_row, column, item)
                        time.sleep(1.1)
                        return True
                    except gspread.exceptions.APIError as e:
                        if tries < sys.getrecursionlimit() and (e.response.json())['error']['code'] == 429:
                            print('hit limit. sleep and then try')
                            time.sleep(501)
                            send_it(tries+1)
                    except Exception as e:
                        print(e)

                send_it()
                column += 1
            next_free_row += 1

    def insert_na(self):
        """Insert missing na rows where data has been skipped for some reason
        find the diff in time between each row

        insert_row - index values is the row number you want to add in.

        Move up the column. Stops the above rows from moving.
        """

        times = self.sheet.col_values(1)[1:]  # Simple list

        times = [datetime.strptime(t, '%Y-%m-%d %H:%M:%S') for t in times]
        index = [*range(2, len(times) + 2)]
        times.reverse()
        index.reverse()
        times = dict(zip(index, times))

        for i, t in times.items():  # +2 to get last item
            n_1_time = times[i - 1]

            diff = int((t - n_1_time).seconds / 3600)
            for r in range(1, diff):
                print(diff, i, t, r)

                def send_it(tries=0):
                    try:
                        self.sheet.insert_row([(t - timedelta(hours=r)).strftime('%Y-%m-%d %H:%M:%S'), "=na()"], index=i,
                                              value_input_option="USER_ENTERED")
                        time.sleep(1.1)
                        return True
                    except gspread.exceptions.APIError as e:
                        if tries < sys.getrecursionlimit() and (e.response.json())['error']['code'] == 429:
                            print('hit limit. sleep and then try')
                            time.sleep(501)
                            send_it(tries+1)
                    except Exception as e:
                        print(e)

                send_it()


def main():
    PostToSheets('GrandfatherClock', '1cB5zOt3oJHepX2_pdfs69tnRl_HBlReSpetsAoc0jVI').insert_na()

    # todo Check current spreadsheet against archive or na()
    # only keep wav files that are not uploaded? 

    if len(sys.argv) > 1:
        wav_file = os.path.abspath(f'{os.path.expanduser("~")}/{sys.argv[1]}')
    else:
        wav_file = os.path.abspath(f'{os.path.expanduser("~")}/chime.wav')
    # fs = 44100

    try:
        drift, actual_time = WaveAnalysis(wav_file, height=200).find_drift()
        actual_time = actual_time.strftime('%Y-%m-%d %H:%M:%S.%f')
        PostToSheets('GrandfatherClock', '1cB5zOt3oJHepX2_pdfs69tnRl_HBlReSpetsAoc0jVI').post_data(
            [[actual_time, drift]])
    except Exception as error:
        print(f"Error at {datetime.now()}: {error}")
        wav_time = WaveAnalysis(wav_file).chime_time()
        os.renames(wav_file, os.path.abspath(f'{os.path.expanduser("~")}/archive/{wav_time.strftime("%Y-%m-%d_%H")}.wav'))


if __name__ == '__main__':
    main()
