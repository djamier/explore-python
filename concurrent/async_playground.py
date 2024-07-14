import asyncio

async def add(a :int, b :int) -> int:
    result = a + b
    await asyncio.sleep(result)
    return result

async def main():
    results = await asyncio.gather(
    add(2, 4),
    add(2, 2),
    add(1, 3)
    )
    # asyncio.even
    return results[0]

if __name__ == '__main__':
    result = asyncio.run(main())
    print(result)