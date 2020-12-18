"""
Keep files where data failed to upload to the google sheet
"""

import os


def remove_excess_files():
    archive = os.path.abspath(f'{os.path.expanduser("~")}/archive/')
    files = os.listdir(archive)
    files = [f for f in files if f.endswith('.wav')]
    files.sort()

    if len(files) > 168:
        os.remove(f'{archive}/{files[0]}')
        remove_excess_files()  # Calls itself recursively to ensure that only 168 files remain.


def main():
    """

    :return:
    """

if __name__ == '__main__':
    main()

