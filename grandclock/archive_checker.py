"""
Removes files from the archive when the number exceeds 1 weeks worth of archive (almost 9GB and 168 items)
"""

import os


def main():
    archive = os.path.abspath(f'{os.path.expanduser("~")}/archive/')
    files = os.listdir(archive)
    files = [f for f in files if f.endswith('.wav')]
    files.sort()

    if len(files) > 168:
        os.remove(f'{archive}/{files[0]}')
        main()  # Calls itself recursively to ensure that only 168 files remain.


if __name__ == '__main__':
    main()

