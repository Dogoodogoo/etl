import math

def convert_to_grid(lat_degree, lon_degree):
    """
    위경도 좌표를 기상청 격자 좌표(nx, ny)로 변환합니다.

    [CS: 알고리즘 - 람베르트 정각원추도법 (Lambert Conformal Conic)]
    지구 구면을 평면상의 격자로 투영하는 복잡한 수학적 변환입니다.
    부동소수점 연산의 정밀도가 결과값에 직접적인 영향을 줍니다.
    """

    # [CS: 상수 관리] 프로그램 전반에서 변하지 않는 물리적 상수를 대문자로 선언하여 가독성과 유지보수성을 높입니다.
    RE = 6371.00877  # 지구 반경(km)
    GRID = 5.0       # 격자 간격(km)
    SLAT1 = 30.0     # 투영 위도 1
    SLAT2 = 60.0     # 투영 위도 2
    OLON = 126.0     # 기준점 경도
    OLAT = 38.0      # 기준점 위도
    XO = 43          # 기준점 X좌표
    YO = 136         # 기준점 Y좌표

    DEGRAD = math.pi / 180.0

    re = RE / GRID
    slat1 = SLAT1 * DEGRAD
    slat2 = SLAT2 * DEGRAD
    olon = OLON * DEGRAD
    olat = OLAT * DEGRAD

    sn = math.tan(math.pi * 0.25 + slat2 * 0.5) / math.tan(math.pi * 0.25 + slat1 * 0.5)
    sn = math.log(math.cos(slat1) / math.cos(slat2)) / math.log(sn)
    sf = math.tan(math.pi * 0.25 + slat1 * 0.5)
    sf = math.pow(sf, sn) * math.cos(slat1) / sn
    ro = math.tan(math.pi * 0.25 + olat * 0.5)
    ro = re * sf / math.pow(ro, sn)

    ra = math.tan(math.pi * 0.25 + (lat_degree) * DEGRAD * 0.5)
    ra = re * sf / math.pow(ra, sn)
    theta = lon_degree * DEGRAD - olon

    if theta > math.pi:
        theta -= 2.0 * math.pi
    if theta < -math.pi:
        theta += 2.0 * math.pi
    theta *= sn

    # [CS: 자료구조 - 결과값 반환]
    # 변환된 좌표를 정수형(int)으로 변환한 뒤, (x, y) 형태의 튜플(Tuple) 구조로 반환합니다.
    nx = math.floor(ra * math.sin(theta) + XO + 0.5)
    ny = math.floor(ro - ra * math.cos(theta) + YO + 0.5)

    return nx, ny