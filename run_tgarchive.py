import sys
from tgarchive import main

args = [
    ["tg-archive", "--new", "--path=lldl"],
    ["tg-archive", "--sync"],
    ["tg-archive", "--build"],
    ["tg-archive", "--build", "-f", "json", "html"],
]

# sys.argv = args[0]
sys.argv = args[1]
main()
sys.argv = args[2]
main()
# sys.argv = args[3]
# main()

