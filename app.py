import sys
import asyncio
from src.usecase.loader import DatabaseLoader
from rich import print


async def main() -> None:
    print("[red]Welcome to Database Loader![/red]")
    print("[red]Now you can load your data from your database to another database[/red]")
    print("[red]. . . . . . . . . . . . . . . . . . . . . . . . . . . .[/red]")
    await DatabaseLoader().run_cli()
    
if __name__ == '__main__':
    try:
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Thank you. 👏")
    