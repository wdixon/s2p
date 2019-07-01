import s2p.cli as cli

# add a wrapper to call the main program which has moved to cli.py in the latest s2p version
if __name__ == "__main__":
    cli.main()
